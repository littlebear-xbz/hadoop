import getopt
import sys
import time

def usage():
    print '''Usage:
    -h: Show help infomation
    -l: Show all table in hbase
    -t {table} show table descriptors
    -t {table} -k {key} : show cell
    -t {table} -k {key} -c {column} : show the coulmn
    -t {table} -k {key} -c {column} -v {version} :show more version
    '''

class get_list_hbase:

    def __init__(self):
        pass

    def get_list_table(self):
        pass

    def get_column_description(self):
        pass

    def get_column_value(self):
        pass

    def get_value_by_key(self):
        pass

    def get_column_version(self):
        pass


def main(argv):
    table_name = ''
    kye = ''
    cloumn = ''
    version = ''

    try:
        opts, args = getopt.getopt(argv, "lht:c:v", ['help','list'])

    except getopt.GetoptError:
        usage()
        sys.exit(2)

    for opt , arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit(2)
        elif opt in ("-l", "--list"):
            pass
        elif opt == '-t':
            table_name = arg

        elif opt == '-k':
            key = arg
        elif opt == '-c':
            cloumn = arg
        elif opt == '-v':
            version = int(arg)

        if (table_name and key and cloumn and version ):
            pass
            sys.exit(0)
        if (table_name and key ):
            pass
        if (table_name):
            pass
    usage()
    sys.exit(1)


if __name__ == '__main__':
    main(sys.argv[1:])