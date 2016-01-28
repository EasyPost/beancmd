import sys
import fnmatch

try:
    import tqdm
except ImportError:
    tqdm = None


def get_tubes(client, initial_tube_list):
    if not initial_tube_list:
        return set(client.list_tubes())
    else:
        server_tubes = client.list_tubes()
        tubes = set()
        for tube in initial_tube_list:
            if '*' in tube or '?' in tube:
                tubes |= set(f for f in server_tubes if fnmatch.fnmatch(f, tube))
            else:
                tubes.add(tube)
        return tubes


def prompt_yesno(prompt_message):
    if sys.version_info < (3, 0):
        response = raw_input(prompt_message).strip()
    else:
        response = input(prompt_message).strip()
    if response != 'y':
        raise ValueError('Got response {0} from prompt, aborting'.format(response))


def progress(iterable, *args, **kwargs):
    if tqdm is not None:
        return tqdm.tqdm(iterable, *args, **kwargs)
    else:
        return iterable
