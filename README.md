# AO3 Backup Utility

A python script that automatically downloads and organizes all the fics an account is subscribed to.

I wrote this because I have an ~~ir~~rational fear of my favorite fics being deleted forever.

### Usage:

Using a virtual environment is recommended **(optional)**:
```
mkdir venv
python3 -m venv venv
source venv/bin/activate
```

Install dependencies:
```
pip install -r requirements.txt
```

Create a `.env` and fill in fields:
```
cp .env_template .env
nano .env
```

Run script:
```
python3 src/main.py
```