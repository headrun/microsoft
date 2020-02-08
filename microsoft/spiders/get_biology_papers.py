import MySQLdb

def main():
    conn = MySQLdb.connect(host='localhost',user='root',passwd='e3e2b51caee03ee85232537ccaff059d167518e2',db='MICROSOFTPAPERSDB',charset='utf8', use_unicode=True)
    cursor = conn.cursor()
    select_query = 'select distinct keywords , sk from papers_info_table where topic=""  order by rand() limit 100000'
    cursor.execute(select_query)
    data = cursor.fetchall()
    sks_list = []
    biology_keyword_list = ['Bioinformatics','Genetics','Ecology','Biotechnology','Microbiology','Molecular biology','Physiology','Neuroscience','Biochemistry','Cell biology','Immunology','Botany','Computational biology','Pharmacology','Evolutionary biology','Toxicology','Zoology','Endocrinology','Anatomy','Virology','Food science','Cancer research','Biophysics','Paleontology','Fishery','Agronomy','Horticulture','Agricultural science','Agroforestry','Animal science','Astrobiology','Biological system','Biology']
    for i in data:
        keywords_list = eval(i[0])
        sk = i[-1]
        for key in keywords_list:
            if key.capitalize() in biology_keyword_list:
                sks_list.append(sk)
    for k in sks_list:
        update_query =  'update papers_info_table set topic="biology" where sk="%s"' % k
        cursor.execute(update_query)
    print(len(sks_list))


if __name__ == "__main__": 
    main()
