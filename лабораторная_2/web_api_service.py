from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from threading import Thread
import time
import json
import os

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://testuser:testpassword@localhost/testdb'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class Entity(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), nullable=False)
    description = db.Column(db.String(200), nullable=True)

with app.app_context():
    db.create_all()

# Временное хранилище записей
pending_entities = []

@app.route('/read', methods=['GET'])
def read():
    entity_id = request.args.get('id')
    entity = Entity.query.get(entity_id)
    if entity:
        return jsonify({'id': entity.id, 'name': entity.name, 'description': entity.description}), 200
    return jsonify({'error': 'Entity not found'}), 404

@app.route('/write', methods=['POST'])
def write():
    data = request.get_json()
    if 'name' not in data or 'description' not in data:
        return jsonify({'error': 'Invalid data'}), 400
    
    new_entity = Entity(name=data['name'], description=data['description'])
    db.session.add(new_entity)
    db.session.commit()

    # Сохраняем в список для записи в файл
    pending_entities.append({'id': new_entity.id, 'name': new_entity.name, 'description': new_entity.description})
    
    return jsonify({'message': 'Entity created', 'id': new_entity.id}), 201

# Функция для записи в файл каждую минуту
def save_entities_periodically():
    while True:
        time.sleep(60)  # ждём 60 секунд
        if pending_entities:
            timestamp = int(time.time())
            filename = f'entities_{timestamp}.json'
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(pending_entities, f, ensure_ascii=False, indent=4)
            print(f"Записали {len(pending_entities)} записей в файл {filename}")
            pending_entities.clear()

# Запускаем фоновый поток для сохранения
thread = Thread(target=save_entities_periodically, daemon=True)
thread.start()

if __name__ == '__main__':
    app.run(debug=True)
