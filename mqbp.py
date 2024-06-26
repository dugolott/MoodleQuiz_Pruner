import json
import mysql.connector

def load_config(filename):
    with open(filename, 'r') as file:
        return json.load(file)

def connect_db(config):
    return mysql.connector.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        database=config['database']
    )

def get_duplicate_questions(conn):
    query = """
    SELECT q1.id as question1_id, q1.nome as question1_nome, q1.categoria as question1_categoria, 
           q1.data as question1_data, q1.autore as question1_autore, q1.descrizione as question1_descrizione,
           q2.id as question2_id, q2.nome as question2_nome, q2.categoria as question2_categoria, 
           q2.data as question2_data, q2.autore as question2_autore, q2.descrizione as question2_descrizione
    FROM questions q1
    JOIN questions q2 ON q1.nome = q2.nome AND q1.categoria = q2.categoria AND q1.data = q2.data 
                      AND q1.autore = q2.autore AND q1.descrizione = q2.descrizione AND q1.id < q2.id;
    """
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchall()

def get_answers(conn, question_id):
    query = """
    SELECT content FROM answers WHERE question_id = %s;
    """
    cursor = conn.cursor()
    cursor.execute(query, (question_id,))
    return [row[0] for row in cursor.fetchall()]

def get_question_related_tables(conn):
    query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = DATABASE() AND table_name LIKE 'question_%';
    """
    cursor = conn.cursor()
    cursor.execute(query)
    return [row[0] for row in cursor.fetchall()]

def update_references(conn, keep_id, duplicate_id):
    related_tables = get_question_related_tables(conn)
    for table in related_tables:
        query = f"""
        UPDATE {table}
        SET question_id = %s
        WHERE question_id = %s;
        """
        cursor = conn.cursor()
        cursor.execute(query, (keep_id, duplicate_id))
    conn.commit()

def delete_duplicate_question(conn, duplicate_id):
    query = """
    DELETE FROM questions
    WHERE id = %s;
    """
    cursor = conn.cursor()
    cursor.execute(query, (duplicate_id,))
    conn.commit()

def manual_choice(conn, duplicates):
    chosen_questions = []
    for dup in duplicates:
        answers1 = get_answers(conn, dup[0])
        answers2 = get_answers(conn, dup[6])
        
        if dup[1:6] == dup[7:12] and answers1 == answers2:
            chosen_questions.append((dup[0], dup[6]))
            print(f"Automatically chosen to keep Question {dup[0]} and delete Question {dup[6]} since all data match.")
        else:
            print(f"{'Question 1':<40}{'Question 2':<40}")
            print(f"{'ID: ' + str(dup[0]):<40}{'ID: ' + str(dup[6]):<40}")
            print(f"{'Nome: ' + dup[1]:<40}{'Nome: ' + dup[7]:<40}")
            print(f"{'Categoria: ' + dup[2]:<40}{'Categoria: ' + dup[8]:<40}")
            print(f"{'Data: ' + dup[3]:<40}{'Data: ' + dup[9]:<40}")
            print(f"{'Autore: ' + dup[4]:<40}{'Autore: ' + dup[10]:<40}")
            print(f"{'Descrizione: ' + dup[5]:<40}{'Descrizione: ' + dup[11]:<40}")
            
            print(f"{'Risposte di Question 1:':<40}{'Risposte di Question 2:':<40}")
            for a1, a2 in zip(answers1, answers2):
                print(f"{a1:<40}{a2:<40}")
            
            choice = input(f"Choose which question to keep (1 or 2): ")
            if choice == '1':
                chosen_questions.append((dup[0], dup[6]))
            elif choice == '2':
                chosen_questions.append((dup[6], dup[0]))
            else:
                print("Invalid choice, skipping these questions.")
    return chosen_questions

def main():
    config = load_config('config.json')
    conn = connect_db(config)
    
    duplicates = get_duplicate_questions(conn)
    chosen_questions = manual_choice(conn, duplicates)
    
    for keep_id, duplicate_id in chosen_questions:
        update_references(conn, keep_id, duplicate_id)
        delete_duplicate_question(conn, duplicate_id)
    
    conn.close()
    print("Duplicate questions processed successfully.")

if __name__ == "__main__":
    main()