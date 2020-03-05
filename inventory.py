from sqlalchemy import Column, Integer, String, DateTime
from base import Base
import datetime


class Inventory(Base):
    """ Inventory """

    __tablename__ = "inventory"

    id = Column(Integer, primary_key=True)
    item_id = Column(String(250), nullable=False)
    name = Column(String(250), nullable=False)
    manufacturer = Column(String(250), nullable=False)
    warehouse = Column(String(250), nullable=False)
    date_created = Column(DateTime, nullable=False)

    def __init__(self, item_id, name, manufacturer, warehouse):
        """ Initializes a inventory reading """
        self.item_id = item_id
        self.name = name
        self.manufacturer = manufacturer
        self.warehouse = warehouse
        self.date_created = datetime.datetime.now()

    def to_dict(self):
        """ Dictionary represenation of a inventory reading """
        dict = {}
        dict['id'] = self.id
        dict['item_id'] = self.item_id
        dict['name'] = self.name
        dict['manufacturer'] = self.manufacturer
        dict['warehouse'] = self.warehouse

        return dict
