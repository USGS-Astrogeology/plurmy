from unittest import mock

import pytest
import plurmy

@pytest.mark.parametrize("time, expected", 
                         [('1:00:00', 3600),
                          ('01:00:00', 3600),
                          ('00:01:00', 60),
                          ('00:00:60', 60),
                          ('00:00:90', 90)
                          ])
def test_slurm_walltime_to_seconds(time, expected):
    assert plurmy.slurm_walltime_to_seconds(time) == expected

@pytest.mark.parametrize("command, output, err, kwargs, expected",
                         [('foo', '', None, {}, 'foo'), # Check command is passing
                          ('foo', '', None, {'name':'SpecialJob'}, 'SpecialJob'),
                          ('foo', '', None, {'time':'00:00:60'}, '00:00:60'),
                          ('foo', '', None, {'outdir':'/mi/casa'}, '/mi/casa'),
                          ('foo', '', None, {'mem':4096}, '4096'),
                          ('foo', '', None, {'queue':'longall'}, 'longall'),
                          ('foo', '', None, {'env':'krc'}, 'krc'),
                          ('foo', '', 'error', {}, 'error')
                         ])
def test_spawn(command, output, err, kwargs, expected):
    with mock.patch('subprocess.Popen') as mock_popen:
        mock_popen.return_value.communicate.return_value = (output, err)
        js = plurmy.spawn(command, **kwargs)
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
                          ('foo', 6, '', None, {'env':'krc'}, 'krc'),
                          ('foo', 2, '', 'error', {}, 'error')
                         ])
def test_job_arr(command, njobs, output, err, kwargs, expected):
    with mock.patch('subprocess.Popen') as mock_popen:
        mock_popen.return_value.communicate.return_value= (output, err)
        js = plurmy.spawn_jobarr(command, njobs, **kwargs)
        if err is None:
            assert mock_popen.call_args_list == [mock.call(['sbatch', '--array', '1-{}'.format(njobs)], stdin=-1, stdout=-1)]
            assert expected in js
        else:
            assert js is False