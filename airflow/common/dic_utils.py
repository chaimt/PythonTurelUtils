import re


def get_dict_value(dict_object, key, default_value):
    result = default_value
    if dict_object is not None and isinstance(dict_object, dict) and key in dict_object.keys():
        result = dict_object[key]

    return result


def get_value_by_path(data, path):
    """
    get value from dict object by its path
    :param data: object that stores the value
    :type data: dict
    :param path: path to the value (set of keys separated by '.'): <key>[.<key>][...]
    :type path: string
    :return: if path is valid returns the corresponding value, otherwise None
    """

    if not isinstance(data, dict) or path == "":
        return None

    value_keys = path.split(".")
    result = data

    for key in value_keys:
        if key in result.keys():
            result = result[key]
        else:
            result = None
            break

    return result


def resolve_placeholders(root):
    """
    will go over all the dictionary and replace placeholders like ${this} according to values
    in the dictionary
    :param root:
    :return:
    """

    def fix_value(value):
        if isinstance(value, str):
            m = re.search("\\${(\\w.*?)\\}", value)
            if m is not None:
                lookup = m.group(1)
                new_value = get_value_by_path(root, lookup)
                if isinstance(new_value, str):
                    lookup_key = "${" + "{value}".format(value=lookup) + "}"
                    new_value = value.replace(lookup_key, new_value)
                return new_value

        return value

    def sub_resolve_placeholders(data):
        if isinstance(data, dict):
            for key in data:
                value = data[key]
                if isinstance(value, str):
                    data[key] = fix_value(value)
                if isinstance(value, dict):
                    sub_resolve_placeholders(value)
                if isinstance(value, list):
                    new_list = sub_resolve_placeholders(value)
                    data[key] = new_list
        if isinstance(data, list) and len(data) > 0:
            new_list = []
            for item in data:
                if isinstance(item, str):
                    fixed_value = fix_value(item)
                    if fixed_value != item:
                        new_list.append(fixed_value)
                    else:
                        new_list.append(item)
                elif isinstance(item, dict):
                    item = sub_resolve_placeholders(item)
                    new_list.append(item)
                else:
                    new_list.append(item)
            return new_list
        return data

    return sub_resolve_placeholders(root)


def dict_merge(dct, merge_dct):
    """Recursive dictionaries merge.

    Go down recursively into dicts nested to an arbitrary depth, updating keys.
    The ``merge_dct`` is merged into ``dct``.

    :param dct: Dictionary onto which the merge is executed.
    :type dct: dict
    :param merge_dct: Dictionary merged into ``dct``.
    :type merge_dct: dict
    """
    if isinstance(dct, dict) and isinstance(merge_dct, dict):
        for k, v in iter(merge_dct.items()):
            if k in dct and isinstance(dct[k], dict) and isinstance(merge_dct[k], dict):
                dict_merge(dct[k], merge_dct[k])
            else:
                dct[k] = merge_dct[k]
