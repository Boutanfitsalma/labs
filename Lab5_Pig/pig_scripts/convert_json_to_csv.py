#!/usr/bin/env python3
"""
Script pour convertir les fichiers JSON en CSV pour Pig
Version pour JSON multi-lignes (pretty-printed)
"""
import json
import csv
import os

def convert_films():
    """Convertir films.json en CSV"""
    films_data = []
    input_file = os.path.join('..', 'data', 'films.json')
    output_file = os.path.join('..', 'data', 'films.csv')
    
    print(f"📖 Lecture de {input_file}...")
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            content = f.read()
            # Diviser par } suivi de { pour séparer les objets JSON
            json_objects = content.replace('}\n{', '}|||{').split('|||')
            
            for i, obj_str in enumerate(json_objects, 1):
                try:
                    film = json.loads(obj_str.strip())
                    films_data.append({
                        'film_id': film['_id'],
                        'title': film['title'].replace(',', ';'),
                        'year': film['year'],
                        'genre': film['genre'],
                        'country': film['country'],
                        'director_id': film['director']['_id']
                    })
                except (json.JSONDecodeError, KeyError) as e:
                    print(f"⚠️  Erreur objet {i}: {e}")
                    continue
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['film_id', 'title', 'year', 'genre', 'country', 'director_id'])
            writer.writeheader()
            writer.writerows(films_data)
        
        print(f"✅ Converti {len(films_data)} films vers {output_file}")
        return True
        
    except FileNotFoundError:
        print(f"❌ Fichier non trouvé: {input_file}")
        return False

def convert_artists():
    """Convertir artists.json en CSV"""
    artists_data = []
    input_file = os.path.join('..', 'data', 'artists.json')
    output_file = os.path.join('..', 'data', 'artists.csv')
    
    print(f"📖 Lecture de {input_file}...")
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            content = f.read()
            json_objects = content.replace('}\n{', '}|||{').split('|||')
            
            for i, obj_str in enumerate(json_objects, 1):
                try:
                    artist = json.loads(obj_str.strip())
                    artists_data.append({
                        'artist_id': artist['_id'],
                        'last_name': artist['last_name'],
                        'first_name': artist['first_name'],
                        'birth_date': artist['birth_date']
                    })
                except (json.JSONDecodeError, KeyError) as e:
                    print(f"⚠️  Erreur objet {i}: {e}")
                    continue
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['artist_id', 'last_name', 'first_name', 'birth_date'])
            writer.writeheader()
            writer.writerows(artists_data)
        
        print(f"✅ Converti {len(artists_data)} artistes vers {output_file}")
        return True
        
    except FileNotFoundError:
        print(f"❌ Fichier non trouvé: {input_file}")
        return False

def convert_actors():
    """Extraire les acteurs des films en CSV"""
    actors_data = []
    input_file = os.path.join('..', 'data', 'films.json')
    output_file = os.path.join('..', 'data', 'film_actors.csv')
    
    print(f"📖 Extraction des acteurs depuis {input_file}...")
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            content = f.read()
            json_objects = content.replace('}\n{', '}|||{').split('|||')
            
            for i, obj_str in enumerate(json_objects, 1):
                try:
                    film = json.loads(obj_str.strip())
                    film_id = film['_id']
                    title = film['title'].replace(',', ';')
                    
                    for actor in film.get('actors', []):
                        actors_data.append({
                            'film_id': film_id,
                            'film_title': title,
                            'actor_id': actor['_id'],
                            'role': actor['role'].replace(',', ';')
                        })
                except (json.JSONDecodeError, KeyError) as e:
                    print(f"⚠️  Erreur objet {i}: {e}")
                    continue
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['film_id', 'film_title', 'actor_id', 'role'])
            writer.writeheader()
            writer.writerows(actors_data)
        
        print(f"✅ Extrait {len(actors_data)} rôles vers {output_file}")
        return True
        
    except FileNotFoundError:
        print(f"❌ Fichier non trouvé: {input_file}")
        return False

if __name__ == '__main__':
    print("🎬 Conversion JSON → CSV pour Apache Pig\n")
    print(f"📂 Dossier courant: {os.getcwd()}\n")
    
    success = True
    success = convert_films() and success
    success = convert_artists() and success
    success = convert_actors() and success
    
    if success:
        print("\n🎉 Conversion terminée avec succès !")
        print("\n📋 Prochaines étapes:")
        print("1. Copier les CSV vers le conteneur:")
        print("   docker cp data/films.csv hadoop-master:/shared_volume/")
        print("   docker cp data/artists.csv hadoop-master:/shared_volume/")
        print("   docker cp data/film_actors.csv hadoop-master:/shared_volume/")
        print("\n2. Dans le conteneur, copier sur HDFS:")
        print("   hdfs dfs -mkdir -p /input/films")
        print("   hdfs dfs -put /shared_volume/films.csv /input/films/")
        print("   hdfs dfs -put /shared_volume/artists.csv /input/films/")
        print("   hdfs dfs -put /shared_volume/film_actors.csv /input/films/")
        print("\n3. Lancer le script Pig:")
        print("   pig /shared_volume/films.pig")
    else:
        print("\n❌ La conversion a échoué. Vérifiez les erreurs ci-dessus.")