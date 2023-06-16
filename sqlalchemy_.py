import json,os
import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship, sessionmaker



Base = declarative_base()

class Publisher(Base):
    __tablename__ = "publisher"
    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.VARCHAR(50), unique = True)

class Book(Base):
    __tablename__ = "book"
    id = sq.Column(sq.Integer, primary_key= True)
    title = sq.Column(sq.VARCHAR(50), nullable = False)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey("publisher.id"), nullable = False)

    publisher = relationship(Publisher, backref = "book")

class Shop(Base):
    __tablename__ = "shop"
    id = sq.Column(sq.Integer, primary_key= True)
    name = sq.Column(sq.VARCHAR(30), unique = True)

class Stock(Base):
    __tablename__ = "stock"
    id = sq.Column(sq.Integer, primary_key= True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey("book.id"),nullable = False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey("shop.id"), nullable = False)
    count = sq.Column(sq.Integer, nullable = False)

    book = relationship(Book, backref = "book")
    shop = relationship(Shop, backref = "shop")

class Sale(Base):
    __tablename__ = "sale"
    id = sq.Column(sq.Integer, primary_key= True)
    price = sq.Column(sq.NUMERIC(10,2), nullable = False)
    date_sale = sq.Column(sq.Date, nullable = False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey("stock.id"), nullable = False)
    count = sq.Column(sq.Integer, nullable= False)

    stock = relationship(Stock, backref = "stock", uselist= False)

def create_tables(engine):
    Base.metadata.create_all(engine)

def insert_data_to_scheme(engine):
    Session = sessionmaker(bind =engine)
    session_1 = Session()
    with open("./test_data.json","r") as f:
        json_body = json.load(f);
    for str in json_body:
        model = globals()[str["model"].capitalize()];
        session_1.add(model(id = str["pk"],**str["fields"]));
    try:
        session_1.commit();
        session_1.close()
        return 'OK'
    except Exception as e:
        session_1.close()
        return f'Error + {repr(e)}'
    
def view_data_of_sales(key,val):
    dict = {'name':Publisher.name, 'id': Publisher.id}
    Session = sessionmaker(bind =engine)
    session_1 = Session()
    q = session_1.query(Publisher.id,Book.title,Shop.name,Sale.price, Sale.date_sale).join(Book).join(Stock).join(Shop, Stock.shop).join(Sale).filter(dict[key] == val)
    session_1.close()
    for i in q.all():
        print(f'{i.title} | {i.name} | {i.price} | {i.date_sale}')

if __name__ == '__main__':
    os.environ["PASS_POSTGRES"]="1234"
    DSN = f"postgresql://postgres:{os.getenv('PASS_POSTGRES')}@localhost:5432/postgres"
    engine = sq.create_engine(DSN)
    create_tables(engine);
    insert_data_to_scheme(engine);
    key = input('Введите поле, по которому будет производиться поиск\n')
    val = input('Введите значение, по которому будет произведен поиске\n')
    view_data_of_sales(key,val)









