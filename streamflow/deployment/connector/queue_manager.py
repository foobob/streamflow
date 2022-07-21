import asyncio
from abc import ABC, abstractmethod
from asyncio import Lock
from asyncio.subprocess import STDOUT
from functools import partial
from typing import Optional, MutableSequence, MutableMapping, Tuple, Any, Union

import cachetools
from cachetools import Cache, TTLCache

from streamflow.core import utils
from streamflow.core.asyncache import cachedmethod
from streamflow.core.scheduling import Location
from streamflow.deployment.connector.ssh import SSHConnector
from streamflow.log_handler import logger

#RBC add
from streamflow.core.exception import WorkflowExecutionException



class QueueManagerConnector(SSHConnector, ABC):

    def __init__(self,
                 deployment_name: str,
                 streamflow_config_dir: str,
                 file: str,
                 hostname: str,
                 username: str,
                 checkHostKey: bool = True,
                 dataTransferConnection: Optional[Union[str, MutableMapping[str, Any]]] = None,
                 maxConcurrentJobs: Optional[int] = 1,
                 passwordFile: Optional[str] = None,
                 pollingInterval: int = 5,
                 sshKey: Optional[str] = None,
                 sshKeyPassphraseFile: Optional[str] = None,
                 transferBufferSize: int = 2 ** 16) -> None:
        super().__init__(
            deployment_name=deployment_name,
            streamflow_config_dir=streamflow_config_dir,
            checkHostKey=checkHostKey,
            dataTransferConnection=dataTransferConnection,
            file=file,
            nodes=[hostname],
            passwordFile=passwordFile,
            sshKey=sshKey,
            sshKeyPassphraseFile=sshKeyPassphraseFile,
            transferBufferSize=transferBufferSize,
            username=username)
        self.hostname: str = hostname
        self.maxConcurrentJobs: int = maxConcurrentJobs
        self.pollingInterval: int = pollingInterval
        self.scheduledJobs: MutableSequence[str] = []
        self.jobsCache: Cache = TTLCache(maxsize=1, ttl=self.pollingInterval)
        self.jobsCacheLock: Lock = Lock()

    @abstractmethod
    async def _get_output(self,
                          job_id: str,
                          location: str) -> str:
        ...

    @abstractmethod
    async def _get_returncode(self,
                              job_id: str,
                              location: str) -> int:
        ...

    @abstractmethod
    async def _get_running_jobs(self,
                                location: str) -> bool:
        ...

    @abstractmethod
    async def _remove_jobs(self,
                           location: str) -> None:
        ...

    @abstractmethod
    async def _run_batch_command(self,
                                 helper_file: str,
                                 job_name: str,
                                 location: str,
                                 workdir: Optional[str] = None,
                                 stdin: Optional[Union[int, str]] = None,
                                 stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                                 stderr: Union[int, str] = asyncio.subprocess.STDOUT) -> str:
        ...

    async def _run(self,
                   location: str,
                   command: MutableSequence[str],
                   environment: MutableMapping[str, str] = None,
                   workdir: Optional[str] = None,
                   stdin: Optional[Union[int, str]] = None,
                   stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                   stderr: Union[int, str] = asyncio.subprocess.STDOUT,
                   job_name: Optional[str] = None,
                   capture_output: bool = False,
                   encode: bool = True,
                   interactive: bool = False,
                   stream: bool = False) -> Union[Optional[Tuple[Optional[Any], int]], asyncio.subprocess.Process]:
        # TODO: find a smarter way to identify detachable jobs when implementing stacked connectors
        if job_name:
            command = utils.create_command(
                command=command,
                environment=environment,
                workdir=workdir)
            logger.debug("Executing command {command} on {location} {job}".format(
                command=command,
                location=location,
                job="for job {job}".format(job=job_name) if job_name else ""))
            helper_file = await self._build_helper_file(command, location, environment, workdir)

# RBC
# this method does deal with any connector errors (e.g. a qsub that failed). I note this here
# because what ever get returned here is assumed to be a valid job_id... 

            job_id = await self._run_batch_command(
                helper_file=helper_file,
                job_name=job_name,
                location=location,
                workdir=workdir,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr)
            logger.info("Scheduled job {job} with job id {job_id}".format(
                job=job_name,
                job_id=job_id))
            self.scheduledJobs.append(job_id)
            async with self.jobsCacheLock:
                self.jobsCache.clear()
            while True:
                async with self.jobsCacheLock:
                    running_jobs = await self._get_running_jobs(location)
#RBC - here is what streamflow thinks the job_id of interest is
#               print(job_id)
#RBC - here is what streamflow thinks its running from qstat
#               print(running_jobs)
                if job_id not in running_jobs:
                    break
                await asyncio.sleep(self.pollingInterval)
            self.scheduledJobs.remove(job_id)
            return (
                await self._get_output(job_id, location) if stdout == STDOUT else None,
                await self._get_returncode(job_id, location))
        else:
            return await super()._run(
                location=location,
                command=command,
                environment=environment,
                workdir=workdir,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr,
                job_name=job_name,
                capture_output=capture_output,
                encode=encode,
                interactive=interactive,
                stream=stream)

    async def get_available_locations(self,
                                      service: str,
                                      input_directory: str,
                                      output_directory: str,
                                      tmp_directory: str) -> MutableMapping[str, Location]:
        return {self.hostname: Location(
            name=self.hostname,
            hostname=self.hostname,
            slots=self.maxConcurrentJobs)}

    async def undeploy(self, external: bool) -> None:
        await self._remove_jobs(self.hostname)
        self.scheduledJobs = {}
        await super().undeploy(external)


class SlurmConnector(QueueManagerConnector):

    async def _get_output(self,
                          job_id: str,
                          location: str) -> str:
        async with self._get_ssh_client(location) as ssh_client:
            output_path = (await ssh_client.run(
                "scontrol show -o job {job_id} | sed -n 's/^.*StdOut=\\([^[:space:]]*\\).*/\\1/p'".format(
                    job_id=job_id
                ))).stdout.strip()
            return ((await ssh_client.run('cat {output}'.format(output=output_path))).stdout.strip()
                    if output_path else "")

    async def _get_returncode(self,
                              job_id: str,
                              location: str) -> int:
        async with self._get_ssh_client(location) as ssh_client:
            return int((await ssh_client.run(
                "scontrol show -o job {job_id} | sed -n 's/^.*ExitCode=\\([0-9]\\+\\):.*/\\1/p'".format(
                    job_id=job_id
                ))).stdout.strip())

    @cachedmethod(lambda self: self.jobsCache, key=partial(cachetools.keys.hashkey, 'running_jobs'))
    async def _get_running_jobs(self,
                                location: str) -> MutableSequence[str]:
        async with self._get_ssh_client(location) as ssh_client:
            return [j.strip() for j in (await ssh_client.run(
                "squeue -h -j {job_ids} -t {states} -O JOBID".format(
                    job_ids=",".join(self.scheduledJobs),
                    states=",".join(['PENDING', 'RUNNING', 'SUSPENDED', 'COMPLETING', 'CONFIGURING',
                                     'RESIZING', 'REVOKED', 'SPECIAL_EXIT'])
                ))).stdout.strip().splitlines()]

    async def _remove_jobs(self,
                           location: str) -> None:
        async with self._get_ssh_client(location) as ssh_client:
            await ssh_client.run(
                "scancel {job_ids}".format(job_ids=" ".join(self.scheduledJobs)))

    async def _run_batch_command(self,
                                 helper_file: str,
                                 job_name: str,
                                 location: str,
                                 workdir: Optional[str] = None,
                                 stdin: Optional[Union[int, str]] = None,
                                 stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                                 stderr: Union[int, str] = asyncio.subprocess.STDOUT) -> str:
        batch_command = "sbatch --parsable {workdir} {stdin} {stdout} {stderr} {helper_file}".format(
            workdir="-D {workdir}".format(workdir=workdir) if workdir is not None else "",
            stdin="-i \"{stdin}\"".format(stdin=stdin) if stdin is not None else "",
            stdout="-o \"{stdout}\"".format(stdout=stdout) if stdout != STDOUT else "",
            stderr="-e \"{stderr}\"".format(stderr=stderr) if stderr != STDOUT and stderr != stdout else "",
            helper_file=helper_file)
        async with self._get_ssh_client(location) as ssh_client:
            result = await ssh_client.run(batch_command)
        return result.stdout.strip()


class PBSConnector(QueueManagerConnector):

    async def _get_output(self,
                          job_id: str,
                          location: str) -> str:
        async with self._get_ssh_client(location) as ssh_client:
#RBC
            print("Check qstat -xf b4 - streamflow believe job has completed and looking for Output_Path")
            print("self.scheduledJobs", self.scheduledJobs)
            print( "qstat {job_id} -xf | sed -n 's/^\\s*Output_Path\\s=\\s.*:\\(.*\\)\\s*$/\\1/p'".format(
                    job_id=job_id
                ))
            output_path = (await ssh_client.run(
                "qstat {job_id} -xf | sed -n 's/^\\s*Output_Path\\s=\\s.*:\\(.*\\)\\s*$/\\1/p'".format(
                    job_id=job_id
                ))).stdout.strip()
            return ((await ssh_client.run('cat {output}'.format(output=output_path))).stdout.strip()
                    if output_path else "")

    async def _get_returncode(self,
                              job_id: str,
                              location: str) -> int:
        async with self._get_ssh_client(location) as ssh_client:
#RBC
            logger.info("RBC - streamflow believes job {job_ids} has completed".format(job_ids=job_id))
            logger.info("RBC - going to sleep for 30 seconds to deal with devel queue race")

            time.sleep(30)
            # assumption here is that qstat will return info on specified job.
            # and returned Exit_status will be changed from what pbs logged (from 0-9) to 1
            logger.info("RBC - qstat {job_ids} -xf | sed -n 's/^\\s*Exit_status\\s=\\s\\([0-9]\\+\\)\\s*$/\\1/p'".format(
                    job_ids=job_id
                ))
            #print("self.scheduledJobs", self.scheduledJobs)
            #print("qstat {job_id} -xf | sed -n 's/^\\s*Exit_status\\s=\\s\\([0-9]\\+\\)\\s*$/\\1/p'".format(
            #       job_id=job_id
            #   ))
            return int((await ssh_client.run(
                # RBC - This code should only be called when the job has correctly been determined to have completed.
                # RBC -- breakage here occurs when a job is either lost for some reason (unlikely), or the 2nd qstat
                # RBC -- issued below can't find the job its looking for. When this happens, this qstat will return a 
                # RBC -- a blank string and barf.... So, its a fragile interface prone to breakage...
                # RBC -- eliminate the -a from qstat because its unecessary and very heavyweight...
                # ORIG  "qstat {job_id} -axf | sed -n 's/^\\s*Exit_status\\s=\\s\\([0-9]\\+\\)\\s*$/\\1/p'".format(
                "qstat {job_id} -xf | sed -n 's/^\\s*Exit_status\\s=\\s\\([0-9]\\+\\)\\s*$/\\1/p'".format(
                    job_id=job_id
                ))).stdout.strip())

    @cachedmethod(lambda self: self.jobsCache, key=partial(cachetools.keys.hashkey, 'running_jobs'))
    async def _get_running_jobs(self,
                                location: str) -> MutableSequence[str]:
        async with self._get_ssh_client(location) as ssh_client:
#RBC
            logger.info("RBC - streamflow checking for job completion - qstat -wx {job_ids}".format(job_ids=self.scheduledJobs))
            #print("Check qstat -wx b4 - streamflow checking for job completion", self.scheduledJobs)
            #print("self.scheduledJobs", self.scheduledJobs)
            #print("qstat -wx {job_ids} | egrep '{grep_ids}' | awk '{{if($10 != \"E\" && $10 != \"F\") {{print $1}}}}'".format(
            #        job_ids=" ".join(self.scheduledJobs),
            #        grep_ids="\\|".join(self.scheduledJobs)))

            return (await ssh_client.run(
                # RBC - this is broken - reversing intent will require some quality lambrusco
                # RBC ORIG "qstat -awx {job_ids} | grep '{grep_ids}' | awk '{{if($10 != \"E\" && $10 != \"F\") {{print $1}}}}'".format(
                # RBC -- everyone has site specific thoughts about what qstat output looks like so we parse for nas.nasa.gov
                # RBC -- output the full request ID for grepping and point to the correct field of interest
                "qstat -W o=+reqid -wx {job_ids} | egrep '{grep_ids}' | awk '{{if($8 != \"E\" && $8 != \"F\") {{print $1}}}}'".format(
                    job_ids=" ".join(self.scheduledJobs),
                    grep_ids="\\|".join(self.scheduledJobs)
                ))).stdout.strip().splitlines()

    async def _remove_jobs(self,
                           location: str) -> None:
        async with self._get_ssh_client(location) as ssh_client:
            await ssh_client.run(
                "qdel {job_ids}".format(job_ids=" ".join(self.scheduledJobs)))

    async def _run_batch_command(self,
                                 helper_file: str,
                                 job_name: str,
                                 location: str,
                                 workdir: Optional[str] = None,
                                 stdin: Optional[Union[int, str]] = None,
                                 stdout: Union[int, str] = asyncio.subprocess.STDOUT,
                                 stderr: Union[int, str] = asyncio.subprocess.STDOUT) -> str:
        batch_command = "{workdir} qsub {stdin} {stdout} {stderr} {helper_file}".format(
            workdir="cd {workdir} &&".format(workdir=workdir) if workdir is not None else "",
            stdin="-i \"{stdin}\"".format(stdin=stdin) if stdin is not None else "",
            stdout=("-o \"{stdout}\"".format(stdout=stdout if stdout != STDOUT else utils.random_name())),
            stderr="-e \"{stderr}\"".format(stderr=stderr) if stderr != STDOUT and stderr != stdout else "",
            helper_file=helper_file)
        async with self._get_ssh_client(location) as ssh_client:
            result = await ssh_client.run(batch_command)
# RBC
#
# there are a couple issues that should be addressed here
#
# 1) the format of the job ID itself can vari depending on qstat output and site specific configurations - so this is broken at the NAS. 
#    I corrected this in the calling routine, but it should likely be reformatted down in these connector specific rountines instead.
#
# 2) qsub can fail and the current code does not check for errors <raspberry>. The code calling also does no result checking.
#    This is probably where error codes and result values need to be checked, as well as a way to gracefully exit on error...
#    I need to see how thats done elsewhere...

        if result.exit_status:
            #print(result.exit_status)
            #print(result.stderr)
            raise WorkflowExecutionException( "exit_status(" + str(result.exit_status) + "): " + result.stderr.strip() )

# RBC
# we reset the generic full job id given by 123456.pbspl1.nas.nasa.gov
# by choping off the .nas.nasa.gov. The reasoning why this is needed is site specific...
# this code needs to be corrected so that we only reach this if the qsub actually resulted
# in a sucessful job subnission.
#           job_id='.'.join(job_id.replace("."," ").split()[0:2])
       
# ORIG  return result.stdout.strip()
#       return result.stdout.strip()
        print( result.stdout.strip())
        return '.'.join(result.stdout.strip().replace("."," ").split()[0:2])
