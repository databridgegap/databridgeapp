#!/bin/bash
#install R
yum install -y R
yum install -y curl-devel
yum install -y libcurl-devel

#install RStudio-Server 1.0.153 (2017-07-20)
wget https://download2.rstudio.org/rstudio-server-rhel-1.0.153-x86_64.rpm
yum install -y --nogpgcheck rstudio-server-rhel-1.0.153-x86_64.rpm
rm rstudio-server-rhel-1.0.153-x86_64.rpm

#install shiny and shiny-server (2017-08-25)
R -e "install.packages('shiny', repos='http://cran.rstudio.com/')"
wget https://download3.rstudio.org/centos5.9/x86_64/shiny-server-1.5.4.869-rh5-x86_64.rpm
yum install -y --nogpgcheck shiny-server-1.5.4.869-rh5-x86_64.rpm
rm shiny-server-1.5.4.869-rh5-x86_64.rpm

mkdir ~/ShinyApps
sudo /opt/shiny-server/bin/deploy-example user-dirs
cp -R /opt/shiny-server/samples/sample-apps/hello ~/ShinyApps/

#add user(s)
useradd SET-YOUR-USERNAME-HERE
echo YOUR-USERNAME:SET-YOUR-PASSWORD-HERE | chpasswd