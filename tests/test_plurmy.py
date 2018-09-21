from unittest import mock

import pytest
import plurmy

@pytest.fixture
def job():
    return(plurmy.Slurm('foo',job_name="SpecialJob", time="00:00:60", output="/mi/casa",
           mem_per_cpu='4096', partition='longall'))

@pytest.mark.parametrize("time, expected", 
                         [('1:00:00', 3600),
                          ('01:00:00', 3600),
                          ('00:01:00', 60),
                          ('00:00:60', 60),
                          ('00:00:90', 90)
                          ])
def test_slurm_walltime_to_seconds(job, time, expected):
    job.time = time
    assert job.slurm_walltime_to_seconds() == expected
@pytest.mark.parametrize("command, output, err, kwargs, expected",
                         [('foo', '', None, {}, 'foo'), # Check command is passing
                          ('foo', '', None, {'name':'SpecialJob'}, 'SpecialJob'),
                          ('foo', '', None, {'time':'00:00:60'}, '00:00:60'),
                          ('foo', '', None, {'outdir':'/mi/casa'}, '/mi/casa'),
                          ('foo', '', None, {'mem':4096}, '4096'),
                          ('foo', '', None, {'queue':'longall'}, 'longall'),
                          ('source activate krc\nfoo', '', None, {'env':'krc'}, 'krc'),
                          ('foo', '', 'error', {}, 'error')
                         ])

def test_spawn(job, command, output, err, kwargs, expected):
    job.command = command
    for k, v in kwargs.items():
        setattr(job, k, v)
    with mock.patch('subprocess.Popen') as mock_popen:
        mock_popen.return_value.communicate.return_value = (output, err)
        js = job.submit()
        if err is None:
            assert expected in js
        else:
            assert js is False

@pytest.mark.parametrize("command, njobs, output, err, kwargs, expected",
                         [('foo', 2, '', None, {}, 'foo'), # Check command is passing
                          ('foo', 3, '', None, {'name':'SpecialJob'}, 'SpecialJob'),
                          ('foo', 4, '', None, {'time':'00:00:60'}, '00:00:60'),
                          ('foo', 5, '', None, {'outdir':'/mi/casa'}, '/mi/casa'),
                          ('foo', 1, '', None, {'mem':4096}, '4096'),
                          ('foo', 6, '', None, {'queue':'longall'}, 'longall'),
                          ('source activate krc\nfoo', 6, '', None, {'env':'krc'}, 'krc'),
                          ('foo', 2, '', 'error', {}, 'error')
                         ])
def test_job_arr(job, command, njobs, output, err, kwargs, expected):
    job.nodes = njobs
    for k, v in kwargs.items():
        setattr(job, k, v)

    with mock.patch('subprocess.Popen') as mock_popen:
        mock_popen.return_value.communicate.return_value= (output, err)
        js = job.submit()
        if err is None:
            assert mock_popen.call_args_list == [mock.call(['sbatch', '--array', '1-{}'.format(njobs)], stdin=-1, stdout=-1)]
            assert expected in js
        else:
            assert js is False
