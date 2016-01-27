import fnmatch


def get_tubes(client, initial_tube_list):
    if initial_tube_list is None:
        return client.list_tubes()
    else:
        server_tubes = client.list_tubes()
        tubes = set()
        for tube in initial_tube_list:
            if '*' in tube or '?' in tube:
                tubes |= set(f for f in server_tubes if fnmatch.fnmatch(f, tube))
            else:
                if tube in server_tubes:
                    tubes.add(tube)
        return tubes
