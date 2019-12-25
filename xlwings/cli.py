import os
import sys
import shutil
import argparse
import xlwings as xw

# Directories/paths
this_dir = os.path.dirname(os.path.realpath(__file__))

if sys.platform.startswith('win'):
    addin_path = os.path.join(os.getenv('APPDATA'), 'Microsoft', 'Excel', 'XLSTART', 'xlwings.xlam')


def addin_install(args):
    if not sys.platform.startswith('win'):
        path = xw.__path__[0] + '/addin/xlwings.xlam'
        print("Cannot install the addin automatically on Mac. Install it via Tools > Excel Add-ins...")
        print("You find the addin here: {0}".format(path))
    else:
        try:
            shutil.copyfile(os.path.join(this_dir, 'addin', 'xlwings.xlam'), addin_path)
            print('Successfully installed the xlwings add-in! Please restart Excel.')
        except IOError as e:
            if e.args[0] == 13:
                print('Error: Failed to install the add-in: If Excel is running, quit Excel and try again.')
            else:
                print(str(e))
        except Exception as e:
            print(str(e))


def addin_remove(args):
    if not sys.platform.startswith('win'):
        print('Error: This command is not available on Mac. Please remove the addin manually.')
    else:
        try:
            os.remove(addin_path)
            print('Successfully removed the xlwings add-in!')
        except WindowsError as e:
            if e.args[0] == 32:
                print('Error: Failed to remove the add-in: If Excel is running, quit Excel and try again.')
            elif e.args[0] == 2:
                print("Error: Could not remove the xlwings add-in. The add-in doesn't seem to be installed.")
            else:
                print(str(e))
        except Exception as e:
            print(str(e))


def addin_status(args):
    if not sys.platform.startswith('win'):
        print('Error: This command is only available on Windows right now.')
    else:
        if os.path.isfile(addin_path):
            print('The add-in is installed at {}'.format(addin_path))
            print('Use "xlwings addin remove" to uninstall it.')
        else:
            print('The add-in is not installed.')
            print('"xlwings addin install" will install it at: {}'.format(addin_path))


def quickstart(args):
    project_name = args.project_name
    cwd = os.getcwd()

    # Project dir
    project_path = os.path.join(cwd, project_name)
    if not os.path.exists(project_path):
        os.makedirs(project_path)
    else:
        sys.exit('Error: Directory already exists.')

    # Python file
    with open(os.path.join(project_path, project_name + '.py'), 'w') as python_module:
        python_module.write('import xlwings as xw\n\n\n')
        if sys.platform.startswith('win'):
            python_module.write('@xw.sub  # only required if you want to import it or run it via UDF Server\n')
        python_module.write('def main():\n')
        python_module.write('    wb = xw.Book.caller()\n')
        python_module.write('    wb.sheets[0].range("A1").value = "Hello xlwings!"\n\n\n')
        if sys.platform.startswith('win'):
            python_module.write('@xw.func\n')
            python_module.write('def hello(name):\n')
            python_module.write('    return "hello {0}".format(name)\n\n\n')
        python_module.write('if __name__ == "__main__":\n')
        python_module.write('    xw.books.active.set_mock_caller()\n')
        python_module.write('    main()\n')

    # Excel file
    if not args.standalone:
        source_file = os.path.join(this_dir, 'quickstart.xlsm')
    elif sys.platform.startswith('win'):
        source_file = os.path.join(this_dir, 'quickstart_standalone_win.xlsm')
    else:
        source_file = os.path.join(this_dir, 'quickstart_standalone_mac.xlsm')
    shutil.copyfile(source_file, os.path.join(project_path, project_name + '.xlsm'))


def runpython_install(args):
    destination_dir = os.path.expanduser("~") + '/Library/Application Scripts/com.microsoft.Excel'
    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)
    shutil.copy(os.path.join(this_dir, 'xlwings.applescript'), destination_dir)
    print('Successfully installed RunPython for Mac Excel 2016!')


def restapi_run(args):
    import subprocess
    try:
        import flask
    except ImportError:
        sys.exit("To use the xlwings REST API server, you need Flask>=1.0.0 installed.")
    host = args.host
    port = args.port

    os.environ['FLASK_APP'] = 'xlwings.rest.api'
    subprocess.check_call(["flask", "run", "--host", host, "--port", port])


def license_update(args):
    """license handler for xlwings PRO and xlwings REPORTS"""
    key = args.key
    if sys.platform.startswith('darwin'):
        config_file = os.path.join(os.path.expanduser("~"), 'Library', 'Containers', 'com.microsoft.Excel', 'Data', 'xlwings.conf')
    else:
        config_file = os.path.join(os.path.expanduser("~"), '.xlwings', 'xlwings.conf')
    license_kv = '"LICENSE_KEY","{0}"\n'.format(key)
    if key:
        # Update xlwings.conf
        new_config = []
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                config = f.readlines()
            for line in config:
                # Remove existing license key and empty lines
                if line.split(',')[0] == '"LICENSE_KEY"' or line in ('\r\n', '\n'):
                    pass
                else:
                    new_config.append(line)
            new_config.append(license_kv)
        else:
            new_config = [license_kv]
        if not os.path.exists(os.path.dirname(config_file)):
            os.makedirs(os.path.dirname(config_file))
        with open(config_file, 'w') as f:
            f.writelines(new_config)

    # Read existing key from config file
    if not os.path.exists(config_file):
        sys.exit('Error: Could not find xlwings.conf. Create it by using the -k/--key option.')
    with open(config_file, 'r') as f:
        config = f.readlines()
    found_key = False
    for line in config:
        if line.split(',')[0] == '"LICENSE_KEY"':
            key = line.split(',')[1].strip()[1:-1]
            found_key = True
    if not found_key:
        sys.exit('Error: Could not find a LICENSE_KEY in xlwings.conf. Add one first by using the -k/--key option.')

    # Update license.lic in licensed packages
    xlwings_reports_lic = os.path.join(os.path.dirname(xw.__file__) + '_reports', 'pytransform', 'license.lic')
    if os.path.exists(xlwings_reports_lic):
        with open(xlwings_reports_lic, 'w') as f:
            f.write(key)
            print("License key successfully updated for xlwings_reports!")
    else:
        sys.exit("Error: Didn't find an installed product that requires a license key.")


def main():
    print('xlwings ' + xw.__version__)
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command')
    subparsers.required = True

    # Add-in
    addin_parser = subparsers.add_parser('addin', help='xlwings Excel Add-in')
    addin_subparsers = addin_parser.add_subparsers(dest='subcommand')
    addin_subparsers.required = True

    addin_install_parser = addin_subparsers.add_parser('install')
    addin_install_parser.set_defaults(func=addin_install)

    addin_update_parser = addin_subparsers.add_parser('update')
    addin_update_parser.set_defaults(func=addin_install)

    addin_upgrade_parser = addin_subparsers.add_parser('upgrade')
    addin_upgrade_parser.set_defaults(func=addin_install)

    addin_remove_parser = addin_subparsers.add_parser('remove')
    addin_remove_parser.set_defaults(func=addin_remove)

    addin_uninstall_parser = addin_subparsers.add_parser('uninstall')
    addin_uninstall_parser.set_defaults(func=addin_remove)

    addin_status_parser = addin_subparsers.add_parser('status')
    addin_status_parser.set_defaults(func=addin_status)

    # Quickstart
    quickstart_parser = subparsers.add_parser('quickstart', help='xlwings quickstart')
    quickstart_parser.add_argument("project_name")
    quickstart_parser.add_argument("-s", "--standalone", action='store_true', help='Include xlwings as VBA module.')
    quickstart_parser.set_defaults(func=quickstart)

    # RunPython (only needed when installed with conda for Mac Excel 2016)
    if sys.platform.startswith('darwin'):
        runpython_parser = subparsers.add_parser('runpython', help='Run this if you installed xlwings via conda and are using Mac Excel 2016')
        runpython_subparser = runpython_parser.add_subparsers(dest='subcommand')
        runpython_subparser.required = True

        runpython_install_parser = runpython_subparser.add_parser('install')
        runpython_install_parser.set_defaults(func=runpython_install)

    # restapi run
    restapi_parser = subparsers.add_parser('restapi',
                                           help='Runs the xlwings REST API via Flask dev server.')
    restapi_subparser = restapi_parser.add_subparsers(dest='subcommand')
    restapi_subparser.required = True

    restapi_run_parser = restapi_subparser.add_parser('run')
    restapi_run_parser.add_argument("-host", "--host", default='127.0.0.1', help='The interface to bind to.')
    restapi_run_parser.add_argument("-p", "--port", default='5000', help='The port to bind to.')
    restapi_run_parser.set_defaults(func=restapi_run)

    # License
    license_parser = subparsers.add_parser('license', help='License key functionality')
    license_subparsers = license_parser.add_subparsers(dest='subcommand')
    license_subparsers.required = True

    license_update_parser = license_subparsers.add_parser('update')
    license_update_parser.add_argument("-k", "--key", help='Provide a new key, otherwise it will take it from the xlwings.conf file.')
    license_update_parser.set_defaults(func=license_update)

    # boilerplate
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
