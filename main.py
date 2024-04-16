from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
import pandas as pd
from sqlalchemy import text

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root@localhost/flaskmysql'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
ma = Marshmallow(app)

class Department(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    department = db.Column(db.String(100), unique=True, nullable=False)

class Hired(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100))
    datetime = db.Column(db.String(100))
    department_id = db.Column(db.Integer, nullable=True)
    job_id = db.Column(db.Integer, nullable=True)
    
class Job(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    job = db.Column(db.String(100), unique=True, nullable=False)



with app.app_context():
    db.create_all()

class DepartmentSchema(ma.Schema):
    class Meta:
        fields = ('id', 'department')

class HiredSchema(ma.Schema):
    class Meta:
        fields = ('id', 'name', 'datetime', 'department_id', 'job_id')

class JobSchema(ma.Schema):
    class Meta:
        fields = ('id', 'job')

department_schema = DepartmentSchema()
departments_schema = DepartmentSchema(many=True)

hire_schema = HiredSchema()
hires_schema = HiredSchema(many=True)

job_schema = JobSchema()
jobs_schema = JobSchema(many=True)

@app.route('/upload_departments', methods=['POST'])
def upload_departments():
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file'}), 400

    try:
        df = pd.read_csv(file, header=None, names=['id','department'])
        for i, row in df.iterrows():
            department = Department(id=row['id'],department=row['department'])
            db.session.add(department)
            db.session.commit()

        return jsonify({'message': 'CSV departments uploaded successfully'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/upload_jobs', methods=['POST'])
def upload_jobs():
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file'}), 400

    try:
        df = pd.read_csv(file, header=None, names=['id','job'])
        for i, row in df.iterrows():
            job = Job(id=row['id'], job=row['job'])
            db.session.add(job)
            db.session.commit()

        return jsonify({'message': 'CSV jobs uploaded successfully'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/upload_employees', methods=['POST'])
def upload_employees():
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file'}), 400
            

    try:
        df = pd.read_csv(file, header=None, names=['id','name','datetime','department_id','job_id'])
        for i, row in df.iterrows():
            job_id = int(row['job_id']) if not pd.isna(row['job_id']) else None
            department_id = int(row['department_id']) if not pd.isna(row['department_id']) else None
            datetime = str(row['datetime']) if not pd.isna(row['datetime']) else None
            name = str(row['name']) if not pd.isna(row['name']) else None
            employee = Hired(id=row['id'], name=name, datetime=datetime, department_id=department_id, job_id=job_id)
            db.session.add(employee)
            db.session.commit()

        return jsonify({'message': 'CSV jobs uploaded successfully'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    




@app.route('/employees_by_job_department', methods=['GET'])
def employees_by_job_department():
    try:
        sql_query = """
            SELECT 
            department,
            job,
            SUM(CASE WHEN quarter=1 THEN conteo ELSE 0 END) AS q1,
            SUM(CASE WHEN quarter=2 THEN conteo ELSE 0 END) AS q2, 
            SUM(CASE WHEN quarter=3 THEN conteo ELSE 0 END) AS q3,
            SUM(CASE WHEN quarter=4 THEN conteo ELSE 0 END) AS q4
            FROM 
            (SELECT department, job, QUARTER(STR_TO_DATE(datetime, '%Y-%m-%dT%H:%i:%sZ')) AS quarter, COUNT(*) AS conteo
            FROM hired
            LEFT JOIN department ON hired.department_id = department.id
            LEFT JOIN job ON hired.job_id = job.id
            WHERE department IS NOT NULL
            AND SUBSTRING(datetime, 1, 4) = '2021'
            GROUP BY 1, 2, 3) AS subq
            GROUP BY 1,2
        """

        result = db.session.execute(text(sql_query))

        names = ['department', 'job', 'q1', 'q2', 'q3', 'q4']
        results = []
        for row in result:
            record_dict = {names[i]: str(value) for i, value in enumerate(row)}
            results.append(record_dict)

        return jsonify(results), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500
    

@app.route('/count_employees_department', methods=['GET'])
def count_employees_department():
    try:
        sql_query = """
            SELECT department.id, department, COUNT(*) hired FROM 
            department 
            LEFT JOIN hired
            ON department.id=hired.department_id
            GROUP BY id, department
            HAVING COUNT(*) > (SELECT AVG(conteo) FROM (SELECT department_id,COUNT(*) conteo FROM
            department 
            LEFT JOIN hired
            ON department.id=hired.department_id
            WHERE SUBSTRING(datetime, 1, 4) = '2021'
            GROUP BY department_id) c_department) 
        """

        result = db.session.execute(text(sql_query))

        names = ['id', 'department', 'hired']
        results = []
        for row in result:
            record_dict = {names[i]: str(value) for i, value in enumerate(row)}
            results.append(record_dict)

        return jsonify(results), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
