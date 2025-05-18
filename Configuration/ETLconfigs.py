
#oracle_engine = create_engine("oracle+cx_oracle://system:admin@localhost:1522/xe")
#mysql_engine = create_engine("mysql+pymysql://root:Kalpesha1%40@localhost:3306/retaildwh", echo=False, future=True)

ORACLE_USER = "system"
ORACLE_PASSWORD = "admin"
ORACLE_HOST = "localhost"
ORACLE_PORT = 1522
ORACLE_SERVICE = "xe"

MYSQL_USER = "root"
MYSQL_PASSWORD = "Kalpesha1%40"
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DATABASE = "retaildwh"

#Linux Machine Configuration

hostname ="192.168.153.144"
username = "kalpesha"
password = "Kalpesha1@"
remote_file_path = "/home/kalpesha/Desktop/sales_data.csv"
local_file_path = "Source_Systems/sales_data_from_Linux_remote.csv"