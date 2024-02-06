import firebase_admin
from firebase_admin import credentials, firestore, db as rt_db, messaging
from dotenv import load_dotenv
import os

load_dotenv()

cred = credentials.Certificate(os.getenv('FIREBASE_CREDENTIALS_PATH'))
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://team-pe-default-rtdb.firebaseio.com/'
})

#referencia a la base de datos RT
ref = rt_db.reference('/')

#instancia de Firebase Store
db = firestore.client()