from eodhd import APIClient
import pandas as pandas
import os
import dotenv

dotenv.load_dotenv()

api = APIClient(os.getenv("EODHD_API_KEY"))

