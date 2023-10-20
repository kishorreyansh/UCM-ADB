from flask import Flask, Response, jsonify, request
from neo4j import GraphDatabase

driver = GraphDatabase.driver(
    uri="neo4j+ssc://82e26e55.databases.neo4j.io",
    auth=("neo4j", "Zz7VyGWgzV81kfJH7tFhi8Ddkxu_BdgfsWpzXmyOf60"),
)

app = Flask(__name__)
@app.route("/", methods=["GET"])
def home():
    return "<h1 style='text-align: center'>Welcome to Film Actors Programming Assignment<h1>"

# Retrieve all the films and shows in database using GET API
@app.route("/film", methods=["GET"])
def films():
    session = driver.session() #setting up the neo4j Database Connection
    query = """MATCH (n:Film) RETURN n"""
    try:
        #we are executing above cypher query to retrieve all films from neo4j database
        results = session.run(query)
        # Empty list to store records
        films = []
        for record in results:
            node = record["n"]
            # we are converting node object to dictonary
            node_dict = dict(node)
            films.append(node_dict)
        # we are converting dictonary into JSON format
        return jsonify(films)
    except Exception as ex:
        print("Exception", ex)
        response = Response(
            "Unable to fetch films !!", status=500, mimetype="application/json"
        )
        return response

# Insert the new film and show using POST API 
@app.route("/film", methods=["POST"])
def insert_film():
    session = driver.session()
    try:
        input_film = request.json
        dynamic_generate_string = (
            "{" + ", ".join([f"{key} : ${key}" for key in input_film.keys()]) + "}"
        )
        query = f"""CREATE (f:Film {dynamic_generate_string}) RETURN f"""
        result = session.run(query, **input_film)
        single_film = result.single()
        if single_film is None:
            return Response(
                "Unable to create film ", status=500, mimetype="application/json"
            )
        return Response(
            "Film inserted successfully !!", status=200, mimetype="application/json"
        )
    except Exception as ex:
        print("Exception")

# Delete the film and show information using title using DELETE API
@app.route("/film/<string:name>", methods=["DELETE"])
def delete_film(name):
    session = driver.session()
    try:
        query = """MATCH (f:Film{title:$name}) DETACH DELETE f"""
        results = session.run(query, name=name)
        return Response(
            "Film Deleted Successfully", status=200, mimetype="application/json"
        )
    except Exception as ex:
        print(ex)
        return Response("Unable to delete film ", status=500, mimetype="application/json")

# Update the film and show information using title. (By update only title, description, and rating) using PATCH API
@app.route("/film/<string:name>", methods=["PATCH"])
def patch_film(name):
    session = driver.session()
    try:
        request_params = request.json
        dynamic_update_film = ", ".join(
            [f"f.{key} = ${key}" for key in request_params.keys()]
        )
        query = f"""MATCH (f:Film {{title: $name}}) SET {dynamic_update_film} RETURN f"""
        result = session.run(query, name=name, **request_params)
        updated_film = result.single()
        if updated_film is None:
            return Response(
                "Film '" + name + "' is not found", status=404, mimetype="application/json"
            )
        return jsonify(dict(updated_film["f"]))
    except Exception as ex:
        print(ex)
        return Response("Unable to Update film", status=500, mimetype="application/json")

# Display the film and show’s detail includes actor’ names using GET API
@app.route("/film/<string:name>", methods=["GET"])
def film_by_name(name):
    session = driver.session()
    query = """MATCH (n:Film{title:$name}) RETURN n"""
    try:
        result = session.run(query, name=name)
        singlefilm = result.single()
        if singlefilm is None:
            return Response(
                "Film " + name + " has not found in Neo4j Database",
                status=404,
                mimetype="application/json",
            )
        # Get all the actors from the film and attach to the film
        query = """MATCH (f:Film {title: $name})<-[:ACTED_IN]-(a:Actor) RETURN a"""
        actoroutput = session.run(query, name=name)
        actors = []
        if actoroutput:
            for actor in actoroutput:
                actors.append(dict(actor["a"]))
        singlefilm = dict(singlefilm["n"])
        singlefilm["actors"] = actors
        return jsonify(singlefilm)
    except Exception as ex:
        print("Exception", ex)
        response = Response(
            "Unable to fetch films by Actors !!", status=500, mimetype="application/json"
        )
        return response


if __name__ == "__main__":
    app.run(port=5000, debug=True)