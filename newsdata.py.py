#!/usr/bin/python3

import psycopg2

# Define queries. Define title, then query data.

query_1_title = ("What are the most popular three articles of all time?")

query_1 = (
    "select articles.title, count(*) as views "
    "from articles inner join log on log.path "
    "like concat('%', articles.slug, '%') "
    "where log.status like '%200%' group by "
    "articles.title, log.path order by views desc limit 3")

query_2_title = ("Who are the most popular article authors of all time?")
query_2 = (
    "select authors.name, count(*) as views from articles inner "
    "join authors on articles.author = authors.id inner join log "
    "on log.path like concat('%', articles.slug, '%') where "
    "log.status like '%200%' group "
    "by authors.name order by views desc")

query_3_title = ("On which days did more than 1% of requests lead to errors?")
query_3 = (
    "select day, perc from ("
    "select day, round((sum(requests)/(select count(*) from log where "
    "substring(cast(log.time as text), 0, 11) = day) * 100), 2) as "
    "perc from (select substring(cast(log.time as text), 0, 11) as day, "
    "count(*) as requests from log where status like '%404%' group by day)"
    "as log_percentage group by day order by perc desc) as final_query "
    "where perc >= 1")

# Open database connection.


def connect(database_name="news"):
    try:
        db = psycopg2.connect("dbname={}".format(database_name))
        cursor = db.cursor()
        return db, cursor
    except:
        print ("Database connection failed, danger will robinson!")

    # Return the results of queries.


def get_query_results(query):
    db, cursor = connect()
    cursor.execute(query)
    return cursor.fetchall()
    # Connection closes.
    db.close()

    # Configure results for display.


def print_query_results(query_results):
    print (query_results[1])
    for index, results in enumerate(query_results[0]):
        print (
            "\t", index+1, "-", results[0],
            "\t - ", str(results[1]), "views")

    # Print errors, if any arise.


def print_error_results(query_results):
    print (query_results[1])
    for results in query_results[0]:
        print ("\t", results[0], "-", str(results[1]) + "% errors")


if __name__ == '__main__':
    # Store output.
    pop_art_res = get_query_results(query_1), query_1_title
    pop_auth_res = get_query_results(query_2), query_2_title
    load_errors = get_query_results(query_3), query_3_title

    # Display output for user.
    print_query_results(pop_art_res)
    print_query_results(pop_auth_res)
    print_error_results(load_errors)
