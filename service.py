from flask import Flask, jsonify, abort, request
from json import dumps
from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:9092'})

app = Flask(__name__)

tasks = [
    {
        'id': 1,
        'title': u'Buy groceries',
        'description': u'Milk, Cheese, Pizza, Fruit, Tylenol',
        'done': False
    },
    {
        'id': 2,
        'title': u'Learn Python',
        'description': u'Need to find a good Python tutorial on the web',
        'done': False
    }
]


@app.route('/todo/api/v1.0/tasks', methods=['GET'])
def get_tasks():
    data = {'tasks': tasks}

    producer.produce('get', value=dumps(data))
    producer.flush()

    return jsonify(data)


@app.route('/todo/api/v1.0/tasks/<int:task_id>', methods=['GET'])
def get_task(task_id):
    task = list(filter(lambda t: t['id'] == task_id, tasks))

    if len(task) == 0:
        abort(404)

    data = {'task': task[0]}

    producer.produce('get-one', value=dumps(data))
    producer.flush()

    return jsonify(data)


@app.route('/todo/api/v1.0/tasks', methods=['POST'])
def create_task():
    if not request.json or 'title' not in request.json:
        abort(400)

    task = {
        'id': tasks[-1]['id'] + 1 if len(tasks) > 0 else 1,
        'title': request.json['title'],
        'description': request.json.get('description', ""),
        'done': False
    }

    tasks.append(task)

    data = {'task': task}

    producer.produce('post', value=dumps(data))
    producer.flush()

    return jsonify(data), 201


@app.route('/todo/api/v1.0/tasks/<int:task_id>', methods=['PUT'])
def update_task(task_id):
    task = list(filter(lambda t: t['id'] == task_id, tasks))

    if len(task) == 0:
        abort(404)

    if not request.json:
        abort(400)

    if 'title' in request.json and type(request.json['title']) != str:
        abort(400)

    if 'description' in request.json and type(request.json['description']) is not str:
        abort(400)

    if 'done' in request.json and type(request.json['done']) is not bool:
        abort(400)

    task[0]['title'] = request.json.get('title', task[0]['title'])
    task[0]['description'] = request.json.get('description', task[0]['description'])
    task[0]['done'] = request.json.get('done', task[0]['done'])

    data = {'task': task[0]}

    producer.produce('put', value=dumps(data))
    producer.flush()

    return jsonify(data)


@app.route('/todo/api/v1.0/tasks/<int:task_id>', methods=['DELETE'])
def delete_task(task_id):
    task = list(filter(lambda t: t['id'] == task_id, tasks))

    if len(task) == 0:
        abort(404)

    tasks.remove(task[0])

    data = {'result': True}

    producer.produce('delete', value=dumps(data))
    producer.flush()

    return jsonify(data)


if __name__ == '__main__':
    app.run(debug=True)
