# config.py
# Owner and Admins
OWNER_ID = 8584039926
ADMIN_IDS = [8104850843]

# Force Join Channels
CHANNELS = [-1003949242183]
CHANNEL_LINKS = [
    "https://t.me/Neural_Backup_Hub",
]

# Log Channel – sirf Number Lookup ke liye
LOG_CHANNELS = {
    'num': -1003482423742,   # Apna actual log channel ID yahan dalen
}

# API Configuration – Only Number Lookup
APIS = {
    'num': {
        'url': 'https://ayaanmods.site/number.php?key=annonymous&number={}',
        'param': 'number',
        'log': LOG_CHANNELS['num'],
        'desc': 'Mobile number lookup',
        'extra_blacklist': ['API_Developer', 'channel_name', 'channel_link']
    },
}

DEV_USERNAME = "@Dev_Afshin"
POWERED_BY = "AFSHIN"
BACKUP_CHANNEL = -1003740236326   # Apna backup channel ID yahan dalen
