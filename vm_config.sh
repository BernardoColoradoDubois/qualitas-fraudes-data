
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
sudo unzip /opt/oracle/instantclient-tools-linux.x64-23.7.0.25.01.zip

sudo sh -c "echo /opt/oracle/instantclient_23_7 > \
      /etc/ld.so.conf.d/oracle-instantclient.conf"

sudo ldconfig

export LD_LIBRARY_PATH=/opt/oracle/instantclient_23_7:$LD_LIBRARY_PATH
export PATH=/opt/oracle/instantclient_23_7:$PATH

sudo apt install nginx