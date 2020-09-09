
cd /home/ec2-user/locustcovid19
docker build -t locustcovid19:latest .
docker run -p 8888:5000 locustcovid19
sleep 2m
sudo shutdown -P now
