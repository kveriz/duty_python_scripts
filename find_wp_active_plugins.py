#!/usr/bin/env pythonbrew-python36
# coding: utf-8

import os
import re
import MySQLdb
import codecs
import mmap
from itertools import islice
from prettytable import PrettyTable
from argparse import ArgumentParser


class FindWpPlugins:
    """
    Search Wordpress active plugin tool
    """

    # change these to your wishness
    MYSQL_DEFAULTS_FILE = "/root/.my.cnf"
    VIRTDOM = "/etc/apache2/virtdom"

    def __init__(self):
        self.args = self.get_args()

    def get_args(self):
        """
        gets arguments from command line
        :return:
        """
        parser = ArgumentParser(description=str(type(self).__doc__))
        parser.add_argument("-v", "--verbose", help="Verbose output", action="store_true")

        return parser.parse_args()

    def mysql_connect(self, database):
        """
        returns the cursor to exact database
        :param database:
        :return:
        """
        connect = MySQLdb.connect(host="localhost",
                                  init_command="USE {}".format(database),
                                  read_default_file=self.MYSQL_DEFAULTS_FILE)

        return connect.cursor()

    def get_configs(self, sites_directories):
        """
        returns list of pair which contains paths to config file and plugin dir
        :param sites_directories:
        :return:
        """
        pairs = []
        for site_dir in sites_directories:
            wp_config = os.path.join(site_dir, 'wp-config.php')
            plugin_dir = os.path.join(site_dir, 'wp-content', 'plugins')

            try:
                per_site = []
                if os.path.exists(wp_config) and os.path.isfile(wp_config) \
                        and os.stat(wp_config).st_size > 0:
                    per_site.append(wp_config)
                if os.path.exists(plugin_dir) and os.path.isdir(plugin_dir) \
                        and os.stat(plugin_dir).st_size > 0:
                    per_site.append(plugin_dir)

                if len(per_site) > 1:
                    pairs.append(per_site)
            except FileNotFoundError:
                pass
            except UnicodeError:
                pass

        return pairs

    def get_mysql_data(self, wp_config):
        """
        returns database and table prefix by config file
        :param wp_config:
        :return:
        """
        customer = wp_config.split("/")[3]
        with codecs.open(wp_config, "r", encoding='utf-8', errors='ignore') as cf:
            db = "NaD"  # Not a Database
            prefix = ''
            for line in cf:
                line = line.strip()
                if line.__contains__("DB_NAME") and line.__contains__(customer):
                    sep_string = line.split(",")[1]
                    # the name of DB must start with customer login
                    # therefore I decided to composite reg exp with it
                    match = re.search('{}([_a-zA-Z0-9]+)'.format(customer), sep_string)
                    if match is not None:
                        db = match.group(0)
                # this check takes place
                # because just checking %_options matches pretty many unwanted tables
                if line.__contains__("$table_prefix") and line.__contains__("="):
                    sep_string = line.split("=")[1]
                    # match strings like "wp_", 'wwp_', '_', '', "wpw_wpw_w_" and so on
                    match = re.match('[\'\"`’](.*)[\'\"`’];', sep_string.strip())
                    if match is not None:
                        prefix = match.group(1)
        return db, prefix

    def parse_vhosts(self, virtdom_path):
        """
        reads every apache2 vhost and extracts document roots
        :param virtdom_path:
        :return:
        """
        sites_directories = set()

        for config in os.listdir(virtdom_path):
            try:
                with open(os.path.join(virtdom_path, config), 'r') as c:
                    with mmap.mmap(c.fileno(), 0, access=mmap.PROT_READ) as m:
                        offset = m.find('DocumentRoot'.encode())

                        if offset == -1:
                            continue

                        m.seek(offset)
                        root = m.readline().decode().split()[1]
                        sites_directories.add(root)
            except ValueError:
                pass

        return sites_directories

    def get_sitename(self, cursor,  options_table):
        """
        returns CMS sitename from database
        :param cursor:
        :param options_table:
        :return:
        """
        try:
            cursor.execute("SELECT option_value FROM {} WHERE option_name = 'siteurl'".format(options_table))
        except (MySQLdb.Error, MySQLdb.Warning) as e:
            return None

        try:
            result = cursor.fetchone()
            return ''.join(result)
        except TypeError:
            return "Siteurl is empty"

    def get_active_plugins(self, cursor,  options_table, plugin_dir):
        """
        gets and returns list of active plugins from database
        :param cursor:
        :param options_table:
        :param plugin_dir:
        :return:
        """
        try:
            """
            The statement returns some data which look like
            option_value
            a:4:{i:0;s:19:"akismet/akismet.php";i:1;s:9:"hello.php";i:2;s:19:
            "jetpack/jetpack.php";i:3;s:27:"woocommerce/woocommerce.php";}
            """
            cursor.execute("SELECT option_value FROM {} WHERE option_name = 'active_plugins'".format(options_table))
            result = cursor.fetchone()
            if result is None:
                return "There are not any active plugins"
            plugins = ''.join(result)

            # separate result
            separate = plugins.split(";")
            active_plugins = []

            for s in separate:
                if s.__contains__(".php"):
                    plugin_path = s.split(":\"")[1]
                    plugin = plugin_path[0:s.split(":\"")[1].__len__() - 1]
                    full_plugin_path = os.path.join(plugin_dir, plugin)
                    if os.path.exists(full_plugin_path) and os.path.isfile(full_plugin_path) \
                            and os.stat(full_plugin_path).st_size > 0:
                        active_plugins.append(full_plugin_path)

            return active_plugins
        except MySQLdb.Error:
            pass

    def show_active_plugins(self, active_plugin_list, db, customer, siteurl):
        """
        gets version, pretty name and returns table of really active plugins
        :param active_plugin_list:
        :param db:
        :param customer:
        :param siteurl:
        :return:
        """
        output_table = PrettyTable()
        output_table.field_names = ["Customer", "Siteurl", "DB", "Plugin", "Version"]
        try:
            for active_plugin in active_plugin_list:
                with codecs.open(active_plugin, "r", encoding='utf-8', errors='ignore') as plugin_file:
                    version = "Undefined"
                    pretty_name = "Unnamed"
                    # the version and pretty name will be in the beginning anyway
                    # there is no need to scan whole content
                    head = list(islice(plugin_file, 60))
                    for line in head:
                        line = line.strip()
                        lower = line.lower()
                        if lower.__contains__("version:") and not lower.__contains__("wp"):
                            version = str.strip((line.split(":")[1]))
                        if lower.__contains__("plugin name:"):
                            sub_list = line.split(":")
                            if len(sub_list) > 2:
                                pretty_name = str.strip(sub_list[-1])
                            else:
                                pretty_name = str.strip(sub_list[1])
                output_table.add_row([customer, siteurl, db, pretty_name, version])

            return output_table
        except FileNotFoundError:
            pass

    def run(self):
        """
        just starts the finding process
        """
        sites_directories = self.parse_vhosts(self.VIRTDOM)
        wp_configs = self.get_configs(sites_directories)

        for conf in wp_configs:
            wp_config = conf[0]
            plugin_dir = conf[1]

            if self.args.verbose:
                print("Config file {}".format(wp_config))
                print("Plugin dir {}".format(plugin_dir))

            customer = wp_config.split("/")[3]

            mysql_data = self.get_mysql_data(wp_config)
            db = mysql_data[0]
            prefix = mysql_data[1]
            options_table = prefix + "options"
            try:
                connect = self.mysql_connect(db)
                sitename = self.get_sitename(connect, options_table)
            except MySQLdb.Error:
                continue
            except TypeError:
                continue
            active_plugins = self.get_active_plugins(connect, options_table, plugin_dir)
            connect.close()
            if active_plugins is not None:
                print(self.show_active_plugins(active_plugins, db, customer, sitename))


if __name__ == '__main__':
    FindWpPlugins().run()
