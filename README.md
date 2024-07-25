# Apache Airflow in WSL (Ubuntu)

## Contents
- [Setting Up Virtual Environment](#setting-up-virtual-environment)
- [Installing Apache Airflow](#installing-apache-airflow)
- [Initializing the Database](#initializing-the-database)
- [Creating Airflow User](#creating-airflow-user)
- [Running Airflow Webserver and Scheduler](#running-airflow-webserver-and-scheduler)
- [Running PySpark in WSL](#running-pyspark-in-wsl)
- [Connecting MySQL Server in Windows and MySQL Client in WSL](#connecting-mysql-server-in-windows-and-mysql-client-in-wsl)

## Setting Up Virtual Environment

First, ensure your system is up to date and install necessary modules:

```bash
sudo apt-get update
sudo apt install python3-venv
sudo apt install python3-pip
sudo apt install python3-dev
```

Create and activate a virtual environment:

```bash
mkdir airflow_wsl   # This will create airflow_wsl directory in /home/user
cd airflow_wsl
python3 -m venv venv       
source venv/bin/activate
```

## Installing Apache Airflow

Before installing Airflow, you need to set up `AIRFLOW_HOME`. If not, the default will be `~/airflow` (e.g., `/home/user/airflow`).

Open your `.bashrc` file using the nano or any other text editor:

```bash
nano ~/.bashrc
```

Add the following lines to the file:

```bash
export AIRFLOW_HOME=/home/user/airflow_wsl
```

> **Note:** You can set `AIRFLOW_HOME` to a directory in the Windows system using `export AIRFLOW_HOME=/mnt/c/Users/<username>/airflow_wsl/`, but it is not recommended beacuse even though files can be accessed across the operating systems, it may significantly slow down performance.

Save (CTRL+S) and exit (CTRL+X) the editor. Then, apply the changes:

```bash
source ~/.bashrc
```

Verify the environment variable:

```bash
echo $AIRFLOW_HOME
```

To navigate to the Airflow directory directly, use:

```bash
cd $AIRFLOW_HOME
```

Install Apache Airflow in the virtual environment created before:

```bash
pip install apache-airflow
```

## Initializing the Database

Initialize the Airflow database:

```bash
airflow db init
```

## Creating Airflow User

Create an admin user for Airflow:

```bash
airflow users create --username admin --password admin --firstname admin --lastname admin --role Admin --email admin@email.com
```

Verify the created user:

```bash
airflow users list
```

## Running Airflow Webserver and Scheduler

Start the Airflow webserver and scheduler:

```bash
airflow webserver 
```

```bash
airflow scheduler
```

Exit by pressing `CTRL+C`.

You have successfully installed Apache Airflow in WSL or any Linux system.

> **Note:** The following steps are optional.

## Running PySpark in WSL

To run PySpark, you need to first install Java:

```bash
sudo apt-get install openjdk-11-jre  # OR
sudo apt-get install openjdk-11-jdk
```

### Setting Environment Variables

Open your `.bashrc` file:

```bash
nano ~/.bashrc
```

Add the following lines to the file:

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

Save and exit the editor. Then, apply the changes:

```bash
source ~/.bashrc
```

Verify the environment variables:

```bash
echo $JAVA_HOME
```

### Install PySpark

Install PySpark:

```bash
pip install pyspark
```

## Connecting MySQL Server in Windows with MySQL Client in WSL

Install MySQL client and necessary dependencies in WSL:

```bash
sudo apt install mysql-client
sudo apt install pkg-config build-essential libmysqlclient-dev
```

Install required Python packages:

```bash
pip install pymysql
pip install apache-airflow-providers-mysql
```

### Making New User in MySQL Server in Windows

Log in to your MySQL server in Windows and create a new user:

```sql
CREATE USER 'wsl_root'@'%' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON *.* TO 'wsl_root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
```

> **Note:** The `%` character allows connections from any IP address. Ensure you use a unique username and a strong password.

These ALTER and DROP statements are for refrences only.

If the user is already created and you want to change the password later:

```sql
ALTER USER 'username' IDENTIFIED BY 'password';
```

To drop unwanted users:

```sql
DROP USER IF EXISTS [username];
DROP USER IF EXISTS '[username]'@'[host]';
```

### Verify Connection

Verify the connection from WSL:

```bash
mysql -u wsl_root -ppassword -h <WSL_IP_address> -P 3306
```

You can find your `<WSL_IP_address>` by typing `ipconfig` in Windows cmd.

If there are connection issues, you might need to adjust firewall settings to allow connections to port 3306.

### Configure Airflow to Use MySQL

Edit `airflow.cfg` to set the MySQL connection string:

```ini
sql_alchemy_conn = mysql+pymysql://wsl_root:password@<WSL_IP_address>:3306/<database>
```

Replace `<database>` with the name of your database. It is recommended to create a new database for Airflow.

Re-initialize the database:

```bash
airflow db init
```

Again create an admin user for airflow as db is changed.

```bash
airflow users create --username admin --password admin --firstname admin --lastname admin --role Admin --email admin@email.com
```
