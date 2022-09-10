import datetime
import os
import re
import time
from dataclasses import asdict, dataclass

import gspread
import requests
import sqlalchemy as sa
import telebot
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from timeloop import Timeloop

SQLALCHEMY_DATABASE_URL = f"postgresql+psycopg2://" \
                          f"{os.environ['POSTGRES_USER']}" \
                          f":{os.environ['POSTGRES_PASSWORD']}" \
                          f"@db/{os.environ['POSTGRES_DB']}"
SHEET_ID = os.environ["SHEET_ID"]
CURRENCY_RATE_API_URL = "http://www.cbr.ru/scripts/XML_daily.asp"
JOB_FREQUENCY_SECOND = int(os.environ["JOB_FREQUENCY_SECOND"])
TELEGRAM_BOT_ID = os.environ["TELEGRAM_BOT_ID"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

telegram_bot = telebot.TeleBot(TELEGRAM_BOT_ID)


@dataclass
class Schema:
    """Dataclass for data extraction."""
    id: int
    order_id: int
    cost_usd: float
    delivery_date: datetime.date
    cost_rur: float | None = None

    def __init__(self, usd_rate: float, *args):
        self.id = int(args[0])
        self.order_id = int(args[1])
        self.cost_usd = float(args[2])
        self.cost_rur = round(float(self.cost_usd) * usd_rate, 2)
        self.delivery_date = datetime.datetime.strptime(args[3], "%d.%m.%Y").date()


engine = sa.create_engine(SQLALCHEMY_DATABASE_URL)
Session = sessionmaker(bind=engine, expire_on_commit=True, autocommit=False, autoflush=True)
Base = declarative_base()


class Test(Base):
    """Test data table model."""
    __tablename__ = "test"

    id = sa.Column(sa.Integer, primary_key=True)
    order_id = sa.Column(sa.Integer, index=True)
    cost_usd = sa.Column(sa.Float(precision=2))
    cost_rur = sa.Column(sa.Float(precision=2))
    delivery_date = sa.Column(sa.Date)

    is_notified = sa.Column(sa.Boolean, default=False)


Base.metadata.create_all(bind=engine)
_db = Session()


def _get_order_data() -> list[list]:
    """
    Extract data from Google sheet.

    :return: list of rows list
    """
    gc = gspread.service_account("creds.json")
    sheet = gc.open_by_key(SHEET_ID)
    worksheet = sheet.sheet1
    return worksheet.get_all_values()


def _get_usd_currency_rate() -> float:
    """
    Get USD rate from crb.ru

    :return: float - USD rate
    """
    xml = requests.get(CURRENCY_RATE_API_URL).text
    usd_str = re.search(r"USD.+?<Value>([^<]+)", xml).group(1)
    return float(usd_str.replace(",", "."))


def _notify_about_delivery_date_expiration(obj: Test):
    """Notice Service."""
    telegram_bot.send_message(TELEGRAM_CHAT_ID, f"Заказ №{obj.id} просрочен. Дата поставки: {obj.delivery_date}.")


def _save_to_db(db: Session, rows: list[list], usd_rate: float) -> None:
    """
    Save updated row to database. And check delivery date expiration.

    :param db: db session
    :param rows: list of data rows
    :param usd_rate: USD rate
    """
    for row in (Schema(usd_rate, *row) for row in rows[1:]):
        obj = db.merge(Test(**asdict(row)))
        db.commit()

        # Check delivery date expiration.
        if not obj.is_notified and obj.delivery_date < datetime.date.today():
            # Notify, if expiration.
            _notify_about_delivery_date_expiration(obj)
            obj.is_notified = True
            db.commit()


tl = Timeloop()


@tl.job(interval=datetime.timedelta(seconds=JOB_FREQUENCY_SECOND))
def job_update_date() -> None:
    """Periodical task for run update data in database."""
    rows = _get_order_data()
    usd_rate = _get_usd_currency_rate()
    _save_to_db(_db, rows, usd_rate)


if __name__ == "__main__":
    tl.start()

    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            tl.stop()
            break
