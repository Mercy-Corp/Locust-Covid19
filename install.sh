
cd /home/ec2-user/locustcovid19
docker build -t locustcovid19:latest .
docker run -p 8888:5000 locustcovid19
#sudo shutdown -P now
