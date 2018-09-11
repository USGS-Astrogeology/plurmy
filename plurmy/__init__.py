import subprocess
import textwrap
import datetime

#TODO: slurm scripts to config

slurm_script = textwrap.dedent("""\
                              #!/bin/bash -l
                              #SBATCH -n 1
                              #SBATCH --mem-per-cpu {}
                              #SBATCH -J {}
                              #SBATCH -t {}
                              #SBATCH -o {}
                              #SBATCH -p {}
                              #SBATCH --exclude=neb[17-20],gpu1
                              source activate {}
                              {}""")

slurm_array = textwrap.dedent("""\
                              #!/bin/bash -l
                              #SBATCH -n 1
                              #SBATCH --mem-per-cpu {}
                              #SBATCH -o {}
                              #SBATCH -J {}
                              #SBATCH -t {}
                              #SBATCH -p {}
                              #SBATCH --exclude=neb[17-20],gpu1
                              source activate {}
                              which python
                              {}
""")

def spawn(command, name='AutoCNet', time='01:00:00', outdir='/home/jlaura/autocnet_server/%j.log', mem=2048, queue='shortall', env='root'):
    """
    f : str
        file path
    """
    process = subprocess.Popen(['sbatch'], stdin=subprocess.PIPE,
                stdout=subprocess.PIPE)
    job_string = slurm_script.format(mem, name, time, outdir, queue, env, command)
    process.stdin.write(str.encode(job_string))
    out, err = process.communicate()
    if err:
        return False

    # If the job log has the %j character, replace it with the actual job id
    #try:
    #job_id = [int(s) for s in out.split() if s.isdigit()][0]
    #job_string = job_string.replace('%j', '{}'.format(job_id))
    #except:
    #    pass
    return job_string

def spawn_jobarr(command, njobs, name='AutoCNet', time='01:00:00',mem=4096, queue='shortall', outdir=r"slurm-%A_%a.out", env='root'):
    
    process = subprocess.Popen(['sbatch', '--array', '1-{}'.format(njobs)],
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE)

    job_string = slurm_array.format(mem, outdir, name, time, queue, env, command)
    process.stdin.write(str.encode(job_string))
    out, err = process.communicate()
    if err:
        return False
    return job_string

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
