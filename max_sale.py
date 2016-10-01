from mrjob.job import MRJob

class MaxSale(MRJob):
    def mapper(self,_,line):
        data = line.strip().split('\t')
        if len(data) == 6:
            store,sale = data[2],data[4]
            if store in ['Reno','Toledo','Chandler']:
                yield(store,float(sale))

    def reducer(self,store,sales):
        store = store
        max_sale = max(sales)
        yield(store,max_sale)


if __name__ == '__main__':
    MaxSale.run()
