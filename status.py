from sqlalchemy import Column, Integer, String, DateTime
from base import Base
import datetime


class Status(Base):
    """ Status """

    __tablename__ = "status"

    id = Column(Integer, primary_key=True)
    item_id = Column(String(250), nullable=False)
    status = Column(String(250), nullable=False)
    destination = Column(String(250), nullable=False)
    deliverydate = Column(DateTime, nullable=False)
    date_created = Column(DateTime, nullable=False)

    def __init__(self, item_id, status, destination, deliverydate):
        """ Initializes a status reading """
        self.item_id = item_id
        self.status = status
        self.destination = destination
        self.deliverydate = datetime.datetime.strptime(deliverydate, '%Y-%m-%dT%H:%M:%S')
        self.date_created = datetime.datetime.now()

    def to_dict(self):
        """ Dictionary representation of a status reading """
        dict = {}
        dict['id'] = self.id
        dict['item_id'] = self.item_id
        dict['status'] = self.status
        dict['destination'] = self.destination
        dict['deliverydate'] = self.deliverydate

        return dict
