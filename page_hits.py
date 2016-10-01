from mrjob.job import MRJob

class PageHits(MRJob):
    def mapper(self,_,line):
        data = line.strip().split()
        if len(data) == 10:
            page = data[6]
            if page == '/assets/js/the-associates.js':
                yield(page,1)

    def reducer(self,page,hits):
        yield(page,sum(hits))


if __name__ == '__main__':
    PageHits.run()
