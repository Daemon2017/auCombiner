import yaml


def get_keys(key_file_path):
    with open(key_file_path) as key_file:
        key_yaml = yaml.safe_load(key_file)
        return key_yaml['access_key_id'], key_yaml['secret_access_key']
