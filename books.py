import sqlalchemy as db
import pandas as pd

engine = db.create_engine('sqlite:///books.db')
connection = engine.connect()
metadata = db.MetaData()

products = db.Table('products', metadata, autoload=True, autoload_with=engine)
books = db.Table('books', metadata, autoload=True, autoload_with=engine)

# case 1-a
result = pd.DataFrame(columns=['Product_Name','Total','Count'])

for i in range(1,11):
    query = db.select([products.columns.product_name, db.func.sum(books.columns.price),
                   db.func.count(books.columns.price)]).where(books.
                                                              columns.status 
                                                              == True).where(books.columns.
                                                                             product_id == i).where(products.
                                                                                                    columns.
                                                                                                    product_id
                                                                                                   ==i)
    
    a = connection.execute(query).fetchall()

    result = result.append([{'Product_Name':a[0][0], 'Total':a[0][1], 'Count':a[0][2]}])
    
result = result.groupby(['Product_Name']).sum()

result['Average'] = [result.Total[i]/result.Count[i] for i in range(len(result))]
result = result.sort_values('Average', ascending=False)

print('Case 1-a Solution','\n-----------------')
print(result)
print('--'*30,'\n')

# case 1-b
query = db.select([db.func.count(books.columns.price), 
                   db.func.sum(books.columns.price)]).where(books.columns.status == True)
                                                                                                
                                                                                               
total_value = connection.execute(query).fetchall()
print('Case 1-b Solution','\n------------\n')
print('Total Number of Sales:', total_value[0][0])
print('Total Revenue:', total_value[0][1]) 
