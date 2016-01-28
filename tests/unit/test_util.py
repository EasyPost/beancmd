import simple_beanstalk

import mock

from beancmd import util


def test_get_tubes_with_all_tubes():
    mock_client = mock.Mock(spec=simple_beanstalk.BeanstalkClient)
    mock_client.list_tubes.return_value = ['foo', 'default']

    assert util.get_tubes(mock_client, []) == set(['foo', 'default'])

    assert util.get_tubes(mock_client, None) == set(['foo', 'default'])


def test_get_tubes_with_wildcard():
    mock_client = mock.Mock(spec=simple_beanstalk.BeanstalkClient)
    mock_client.list_tubes.return_value = ['foo', 'default']

    assert util.get_tubes(mock_client, ['f*']) == set(['foo'])


def test_get_tubes_with_exact_list():
    mock_client = mock.Mock(spec=simple_beanstalk.BeanstalkClient)
    mock_client.list_tubes.return_value = ['foo', 'bar', 'default']

    assert util.get_tubes(mock_client, ['bar', 'foo']) == set(['foo', 'bar'])
