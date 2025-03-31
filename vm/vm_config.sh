sudo apt update
sudo apt upgrade
sudo apt install python-is-python3
sudo apt install python3-pip
sudo apt install build-essential libssl-dev libffi-dev python3-setuptools
sudo apt install python3.12-venv

sudo apt install unzip
sudo apt install curl
sudo apt install git
sudo apt install nano
sudo apt install wget

curl -O http://launchpadlibrarian.net/648013231/libtinfo5_6.4-2_amd64.deb
sudo dpkg -i libtinfo5_6.4-2_amd64.deb

curl -O http://launchpadlibrarian.net/648013227/libncurses5_6.4-2_amd64.deb
sudo dpkg -i libncurses5_6.4-2_amd64.deb

curl -O http://launchpadlibrarian.net/646633572/libaio1_0.3.113-4_amd64.deb
sudo dpkg -i libaio1_0.3.113-4_amd64.deb

sudo mkdir /opt/oracle

cd /opt/oracle 

sudo wget https://download.oracle.com/otn_software/linux/instantclient/2370000/instantclient-basic-linux.x64-23.7.0.25.01.zip
sudo wget https://download.oracle.com/otn_software/linux/instantclient/2370000/instantclient-sqlplus-linux.x64-23.7.0.25.01.zip
sudo wget https://download.oracle.com/otn_software/linux/instantclient/2370000/instantclient-tools-linux.x64-23.7.0.25.01.zip

sudo unzip /opt/oracle/instantclient-basic-linux.x64-23.7.0.25.01.zip
sudo unzip /opt/oracle/instantclient-sqlplus-linux.x64-23.7.0.25.01.zip
sudo unzip -N /opt/oracle/instantclient-tools-linux.x64-23.7.0.25.01.zip

sudo sh -c "echo /opt/oracle/instantclient_23_7 > \
      /etc/ld.so.conf.d/oracle-instantclient.conf"

sudo ldconfig

export LD_LIBRARY_PATH=/opt/oracle/instantclient_23_7:$LD_LIBRARY_PATH
export PATH=/opt/oracle/instantclient_23_7:$PATH

sudo sh -c 'cat > /etc/profile.d/oracle.sh << EOF
export ORACLE_HOME=/opt/oracle/instantclient_*
export LD_LIBRARY_PATH=\$ORACLE_HOME:\$LD_LIBRARY_PATH
export PATH=\$ORACLE_HOME:\$PATH
EOF'

sudo chmod +x /etc/profile.d/oracle.sh

sudo apt install nginx

sudo systemctl restart networkd-dispatcher.service
sudo systemctl restart unattended-upgrades.service
sudo systemctl restart nginx

sudo groupadd appusers
sudo usermod -a -G appusers bernardo_colorado
sudo usermod -a -G appusers ubuntu

sudo git clone https://github.com/BernardoColoradoDubois/qualitas-fraudes-data /opt/qualitas-fraudes-data

nano /opt/qualitas-fraudes-data/.env
# agregar las variables de entorno
sudo mkdir -p /opt/keys
# agregar las credenciales json de google cloud

sudo python -m venv /opt/qualitas-fraudes-data/venv

sudo chmod -R 775 /opt/qualitas-fraudes-data
sudo chmod -R 775 /opt/keys
sudo chown -R root:appusers /opt/qualitas-fraudes-data
sudo chown -R root:appusers /opt/keys
sudo chmod g+s /opt/qualitas-fraudes-data
sudo chmod g+s /opt/keys

source /opt/qualitas-fraudes-data/venv/bin/activate
pip install -r /opt/qualitas-fraudes-data/requirements.txt

sudo nano /etc/nginx/sites-available/miapp
# agregar la configuración del servidor

sudo nano /etc/systemd/system/miapp.service
# agregar el archivo de configuración del servicio


