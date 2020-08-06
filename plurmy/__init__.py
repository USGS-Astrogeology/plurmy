import subprocess
import datetime



def slurm_walltime_to_seconds(walltime):
    """
    Convert a slurm defined walltime in the form
    HH:MM:SS into a number of seconds.

    Parameters
    ----------
    walltime : str
            In the form HH:MM:SS

    Returns
    -------
    d : int
        The number of seconds the walltime represents

    Examples
    >> walltime = '01:00:00'
    >> sec = slurm_walltime_to_seconds(walltime)
    >> sec
    3600
    """
    walltime = walltime.split(':')
    walltime = list(map(int,walltime))
    d = datetime.timedelta(hours=walltime[0],
                    minutes=walltime[1],
                    seconds=walltime[2])

    return d.seconds

class Slurm(object):
    def __init__(self, command, job_name=None, time="01:00:00", output=None, mem_per_cpu=2048, nodes=None, partition=None):
        self.command = command
        self.job_name = job_name
        self.time = time
        self.output = output
        self.mem_per_cpu = mem_per_cpu
        self.nodes = nodes
        self.partition = partition

    @property
    def job_name(self):
        return getattr(self, '_job_name', None)


    @job_name.setter
    def job_name(self, job_name):
        self._job_name = job_name


    @property
    def time(self):
        return getattr(self, '_time', None)


    @time.setter
    def time(self, time):
        self._time = time


    @property
    def output(self):
        return getattr(self, '_output', None)


    @output.setter
    def output(self, output):
        output = '{}{}'.format(output, '.%A_%a.out')
        self._output = output


    @property
    def mem_per_cpu(self):
        return getattr(self, '_mem_per_cpu', 2048)


    @mem_per_cpu.setter
    def mem_per_cpu(self, mem):
        self._mem_per_cpu = mem


    @property
    def partition(self):
        return getattr(self, '_partition', None)


    @partition.setter
    def partition(self, partition):
        self._partition = partition


    @property
    def nodes(self):
        return getattr(self, '_nodes', None)


    @nodes.setter
    def nodes(self, nodes):
        self._nodes = nodes

    @property
    def command(self):
        return getattr(self, '_command')


    @command.setter
    def command(self, command):
        self._command = command

    def _submit_one(self):
        proc = ['sbatch']
        process = subprocess.Popen(proc, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        job_str = str(self)
        process.stdin.write(job_str.encode())
        out, err = process.communicate()
        if err:
            return False
        return job_str

    def submit(self, array=None, chunksize=1000, exclude=None):
        """ Submits the slurm job.

        Parameters
        ----------
        array : str
                The Slurm formatted specification that describes array attributes.

        Returns
        -------
        job_str : str
            The string representation of the sbatch file submitted to slurm.


        Examples
        --------
        slurm_job = Slurm('./foo.py')
        slurm_job.submit()
        slurm_job.submit("1-6")
        slurm_job.submit("1-3, 8-9")
        """

        if array == None:
            return self._submit_one()
        else:
            job_strs = []
            arrays = []
            # Parse out the array max concurrent job string
            if '%' in array:
                array, step = array.split('%')
            else:
                step = None
            # Parse if different array job ids have been manually specified
            splits = array.split(',')
            for extent in splits:
                # Map the strings to ints for math
                try:
                    start, stop = list(map(int, extent.split('-')))
                except:
                    return self._submit_one()
                # Case where the total number of jobs is > the chunk size
                if stop - start + 1 > chunksize:
                    quot, rem = divmod(stop-start+1, chunksize)
                    arrays = [f'1-{chunksize}']
                    if step:
                        arrays[0] += f'%{step}'
                    arrays = arrays*quot
                    if rem:
                        remainder = f'1-{rem}'
                        if step:
                            remainder += f'%{step}'
                        arrays.append(remainder)
                # Total number of jobs is < the chunk size
                else:
                    inbounds = f'{start}-{stop}'
                    if step:
                        inbounds += f'%{step}'
                    arrays.append(inbounds)

            if exclude is not None:
                proc = ['sbatch', '--exclude={}'.format(exclude)]
            else:
                proc = ['sbatch']

            for array in arrays:
                proc = ['sbatch']
                proc.extend(('--array', array))

                process = subprocess.Popen(proc, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
                job_str = str(self)
                job_strs.append(job_str)
                process.stdin.write(job_str.encode())
                out, err = process.communicate()
                if err:
                    return False
        return job_strs

    def __repr__(self):
        sbatch = '#SBATCH --{}{}'
        cmd = ['#!/bin/bash -l']
        for k, v in vars(self).items():
            if k.startswith('__') or k == '_command':
                continue
            else:
                # Dict items are prefixed with '_' -- get rid of it
                k = k[1:] if k.startswith('_') else k
                if v is not None:
                    # Convert python style separators (_) to slurm separators.
                    k = k.replace('_','-')
                    # Allow flag specification using empty string.  Necessary
                    # to support flags like '--test'
                    #  that have no arguments.
                    v = '{sep}{val}'.format(sep = '=' if str(v) else '', val=v)
                    cmd.append(sbatch.format(k, v))

        cmd.append(self.command)
        return '\n'.join(str(s) for s in cmd)
