import pyarrow.dataset as ds

dataset = ds.dataset(
    'E:\\Projects\\HolmesLM\\dataset\\4_5',
    format='parquet')
    # filters=[('source', '!=', 'SkyPile')])
    
scanner = ds.Scanner.from_dataset(dataset, columns=['text'], filter=ds.field('source') != 'SkyPile')

table = scanner.take(range(0, 10))

print(next(iter(table.column('text'))))

# batches = dataset.to_batches(columns=['text'], filter=ds.field('source') != 'SkyPile')

# print(next(batches).to_pandas())