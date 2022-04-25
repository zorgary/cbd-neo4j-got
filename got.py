#!/usr/bin/env python
from json import dumps
import logging

from flask import (
    Flask,
    g,
    request,
    Response,
)
from neo4j import (
    GraphDatabase,
    basic_auth,
)

app = Flask(__name__, static_url_path="/static/")
url = "neo4j://localhost:7687"
username = "neo4j"
password = "cbdneo4j"
neo4j_version = "4"
database = "neo4j"

port = 8080

driver = GraphDatabase.driver(url, auth=basic_auth(username, password))


def get_db():
    if not hasattr(g, "neo4j_db"):
        if neo4j_version.startswith("4"):
            g.neo4j_db = driver.session(database=database)
        else:
            g.neo4j_db = driver.session()
    return g.neo4j_db


@app.teardown_appcontext
def close_db(error):
    if hasattr(g, "neo4j_db"):
        g.neo4j_db.close()


@app.route("/")
def get_index():
    return app.send_static_file("index.html")


def serialize_house(house):
    return {
        "id": house["id"],
        "name": house["name"],
        "coatOfArms": house["coatOfArms"],
        "words": house["words"],
        "founded": house["founded"],
        "titles": house["titles"],
        "ancestralWeapons": house["ancestralWeapons"],
        "region": house["region"],
        "seats": house["seats"]
    }


def serialize_region(region):
    return {
        "id": region["id"],
        "name": region["name"]
    }

def serialize_seat(seat):
    return {
        "id": seat["id"],
        "name": seat["name"]
    }

def serialize_person(person):
    return {
        "id": person["id"],
        "aliases": person["aliases"],
        "books": person["books"],
        "tvSeries": person["tvSeries"],
        "playedBy": person["playedBy"],
        "isFemale": person["isFemale"],
        "culture": person["culture"],
        "died": person["died"],
        "title": person["title"],
    }

@app.route("/list")
def get_list():
    def work(tx):
        return list(tx.run(
            "MATCH (n:Person)-[r:ALLIED_WITH]->(h:House) "
            "RETURN h AS house, SUM(SIZE(n.tvSeries)) "
            "ORDER BY SUM(SIZE(n.tvSeries)) DESC "
            "LIMIT 10"
        ))
    db = get_db()
    results = db.read_transaction(work)
    return Response(
        dumps([serialize_house(record["house"]) for record in results]),
        mimetype="application/json"
    )

@app.route("/allies/<house_id>", methods=["GET"])
def get_allies(house_id):
    def work(tx):
        return list(tx.run(
            "MATCH (p:Person)-[r:ALLIED_WITH]->(h:House) "
            "WHERE h.id = toInteger($house) "
            "RETURN COUNT(p) AS num_allies",
            {"house": house_id}
        ))
    db = get_db()
    results = db.read_transaction(work)
    return Response(
        dumps(results[0]["num_allies"]),
        mimetype="application/json"
    )

if __name__ == "__main__":
    logging.root.setLevel(logging.INFO)
    logging.info("Starting on port %d, database is at %s", port, url)
    app.run(port=port, use_reloader=True)