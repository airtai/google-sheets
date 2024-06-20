from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import relationship

from .database import Base


class Auth(Base):  # type: ignore[misc]
    __tablename__ = "auth"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("user.id"))

    items = relationship("Item", back_populates="owner")
