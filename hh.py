#!/usr/bin/env python
import sys
import os
import argparse
import subprocess
import shutil
import re
from urlparse import urljoin
from urllib import urlretrieve

import requests


AVRO_TOOLS_BASE_URL = 'http://www.us.apache.org/dist/avro/stable/java/'
RE_AVRO_TOOLS = re.compile(r'\b(avro-tools-[\d\.]+\.jar)\b')
BASE_PATH = os.path.expanduser('~')


class Hadoop(object):

    def _get_cmd(self, args):
        return ['hadoop', 'fs'] + args

    def _clean(self, filename):
        if os.path.exists(filename):
            if os.path.isdir(filename):
                shutil.rmtree(filename)
            else:
                os.remove(filename)

    def _iter_files(self, path_root):
        if os.path.isfile(path_root):
            yield path_root
        else:
            for path, dirs, files in os.walk(path_root):
                for file_ in files:
                    yield os.path.join(path, file_)

    def _convert(self, path, converters, remove_source=False):
        for src_file in self._iter_files(path):
            converter = converters.get(os.path.splitext(src_file)[1])
            if not converter:
                continue
            dst_file = converter(src_file)
            if not dst_file:
                continue
            print 'converted %s to %s' % (src_file, dst_file)
            if remove_source:
                os.remove(src_file)
                print 'removed %s' % src_file

    def get(self, src_files, converters=None, remove_source=True):
        if not isinstance(src_files, (list, tuple)):
            src_files = [src_files]

        for src_file in src_files:
            src_file = src_file.rstrip('/')
            dst = os.path.basename(src_file)
            self._clean(dst)

            cmd = self._get_cmd(['-get', src_file])
            print ' '.join(cmd)
            stdout, stderr, return_code = popen(cmd, print_output=True)
            if return_code != 0:
                continue

            if converters and isinstance(converters, dict):
                self._convert(dst, converters=converters,
                        remove_source=remove_source)

    def put(self, src_files, dst_path):
        for src_file in src_files:
            if not os.path.exists(src_file):
                print '%s does not exist' % src_file
                continue

            src_filename = os.path.basename(src_file)
            dst_file = os.path.join(dst_path, src_filename)
            for cmd in (
                    self._get_cmd(['-rm', dst_file]),
                    self._get_cmd(['-put', src_file, dst_path]),
                    ):
                print ' '.join(cmd)
                stdout, stderr, return_code = popen(cmd, print_output=True)


class AvroTools(object):

    def __init__(self):
        self.avro_tools = self._get_file()

    def _get_local_file(self):
        for filename in os.listdir(BASE_PATH):
            if RE_AVRO_TOOLS.search(filename):
                return os.path.join(BASE_PATH, filename)

    def _get_remote_file(self):
        res = requests.get(AVRO_TOOLS_BASE_URL)
        match = RE_AVRO_TOOLS.search(res.text)
        if not match:
            raise Exception('failed to find avro-tools')

        filename = match.group(0)
        url = urljoin(AVRO_TOOLS_BASE_URL.rstrip('/') + '/', filename)
        dst_filename = os.path.join(BASE_PATH, filename)
        if not os.path.exists(dst_filename):
            print 'downloading %s to %s' % (url, dst_filename)
            urlretrieve(url, dst_filename)
        return dst_filename

    def _get_file(self):
        return self._get_local_file() or self._get_remote_file()

    def convert_to_json(self, avro_file):
        cmd = ['java', '-jar', self.avro_tools, 'tojson', avro_file]
        print ' '.join(cmd)
        stdout, stderr, return_code = popen(cmd)
        if return_code != 0:
            if stdout:
                sys.stdout.write(stdout)
            if stderr:
                sys.stdout.write(stderr)
            return None

        json_file = '%s.json' % os.path.splitext(avro_file)[0]
        with open(json_file, 'wb') as fd:
            fd.write(stdout)
        return json_file


def popen(cmd, print_output=False):
    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
        stdout, stderr = proc.communicate()
        if print_output:
            if stdout:
                sys.stdout.write(stdout)
            if stderr:
                sys.stdout.write(stderr)
        return stdout, stderr, proc.returncode
    except Exception, e:
        print 'failed to execute "%s": %s' % (' '.join(cmd), str(e))
        return None, None, None

def get(args):
    avro = AvroTools()
    converters = {
        '.avro': avro.convert_to_json,
        }
    Hadoop().get(args.paths, converters=converters)

def put(args):
    if len(args.paths) < 2:
        print 'missing destination path'
        return
    dst_path = '/%s/' % args.paths[-1].strip('/')
    Hadoop().put(args.paths[:-1], dst_path)

def main():
    parser = argparse.ArgumentParser(description='HDFS helpers')
    subparsers = parser.add_subparsers()

    parser_get = subparsers.add_parser('get')
    parser_get.add_argument('paths', nargs='+', type=str, help='paths')
    parser_get.set_defaults(func=get)

    parser_put = subparsers.add_parser('put')
    parser_put.add_argument('paths', nargs='+', type=str, help='paths')
    parser_put.set_defaults(func=put)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
