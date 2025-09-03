# app.py - Main Flask Application

import logging
import os
import uuid
from datetime import datetime
from typing import Dict, Any

import pandas as pd
from flask import Flask, request, jsonify, send_file, current_app, send_from_directory
from flask_cors import CORS

from models.execution_models import ExecutionStatus
from models.pipeline_models import PipelineDefinition, ExecutionRequest
from services.file_service import FileService
from services.ocel_service import OCELService
from services.pipeline_service import PipelineService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for Angular frontend

# Configuration
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024  # 500MB max file size
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['EXPORT_FOLDER'] = 'exports'
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key')

# Ensure upload and export directories exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['EXPORT_FOLDER'], exist_ok=True)

# Services
pipeline_service = PipelineService()
file_service = FileService(app.config['UPLOAD_FOLDER'])
ocel_service = OCELService(app.config['EXPORT_FOLDER'])

# In-memory execution tracking (in production, use Redis or database)
executions: Dict[str, Dict[str, Any]] = {}


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0'
    })


@app.route('/api/dataset/upload', methods=['POST'])
def upload_dataset():
    """Upload a dataset file to the server."""
    try:
        if 'dataset' not in request.files:
            return jsonify({'error': 'No file provided'}), 400

        file = request.files['dataset']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400

        # Get additional metadata
        file_type = request.form.get('fileType', 'CSV')
        original_filename = request.form.get('fileName', file.filename)

        # Save file
        result = file_service.save_uploaded_file(file, file_type, original_filename)

        logger.info(f"Dataset uploaded: {result['filename']}")

        return jsonify({
            'success': True,
            'fileId': result['file_id'],
            'filename': result['filename'],
            'originalName': original_filename,
            'fileType': file_type,
            'size': result['size'],
            'uploadedAt': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Dataset upload error: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/dataset/<file_id>', methods=['GET'])
def get_dataset_info(file_id: str):
    """Get information about an uploaded dataset."""
    try:
        info = file_service.get_file_info(file_id)
        if not info:
            return jsonify({'error': 'Dataset not found'}), 404

        return jsonify(info)

    except Exception as e:
        logger.error(f"Dataset info error: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/pipeline/validate', methods=['POST'])
def validate_pipeline():
    """Validate a pipeline definition."""
    try:
        pipeline_data = request.get_json()
        if not pipeline_data:
            return jsonify({'error': 'No pipeline data provided'}), 400

        # Parse pipeline definition
        pipeline = PipelineDefinition.parse_obj(pipeline_data)

        # Validate pipeline
        validation_result = pipeline_service.validate_pipeline(pipeline)

        return jsonify({
            'isValid': validation_result.is_valid,
            'errors': validation_result.errors,
            'warnings': validation_result.warnings,
            'validatedAt': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Pipeline validation error: {str(e)}")
        return jsonify({
            'isValid': False,
            'errors': [str(e)],
            'warnings': []
        }), 400


@app.route('/api/pipeline/execute', methods=['POST'])
def execute_pipeline():
    """Execute a pipeline."""
    try:
        pipeline_data = request.get_json()
        if not pipeline_data:
            return jsonify({'error': 'No pipeline data provided'}), 400

        # Parse pipeline definition
        pipeline = PipelineDefinition.parse_obj(pipeline_data)

        # Create execution request
        execution_request = ExecutionRequest(
            pipeline=pipeline,
            execution_id=str(uuid.uuid4()),
            user_id=request.headers.get('X-User-ID', 'anonymous'),
            metadata={}
        )

        # Start execution
        logger.info(f"Starting pipeline execution: {execution_request.execution_id}")

        # Initialize execution tracking
        executions[execution_request.execution_id] = {
            'status': ExecutionStatus.RUNNING,
            'startedAt': datetime.now().isoformat(),
            'pipeline': pipeline_data,
            'logs': [],
            'results': {}
        }

        # Execute pipeline
        try:
            execution_result = pipeline_service.execute_pipeline(execution_request)

            # Update execution tracking
            executions[execution_request.execution_id].update({
                'status': ExecutionStatus.COMPLETED if execution_result.success else ExecutionStatus.FAILED,
                'completedAt': datetime.now().isoformat(),
                'results': execution_result.results,
                'logs': execution_result.logs,
                'errors': execution_result.errors
            })

            response_data = {
                'success': execution_result.success,
                'executionId': execution_request.execution_id,
                'results': execution_result.results,
                'logs': execution_result.logs,
                'errors': execution_result.errors,
                'completedAt': datetime.now().isoformat()
            }

            for key, value in response_data['results']["node_results"].items():
                if type(value) is pd.DataFrame:
                    response_data['results']["node_results"][key] = value.head(100).to_dict(orient='records')

            response_data["results"] = {}
            exec_test = list(execution_result.results["node_results"].values())
            pd_res = list(filter(lambda n: type(n) == dict and n["type"] == "process_discovery" , exec_test))

            ocel_res = list(filter(lambda n: type(n) == dict and n["type"] == "table" , exec_test))

            if pd_res and len(pd_res) > 0:
                pd_entry = pd_res[0]
                response_data["results"]["process_discovery"] = pd_entry["config"]["path"]

            if ocel_res and len(ocel_res) > 0:
                ocel_entry = ocel_res[0]
                response_data["results"]["table"] = ocel_entry["config"]["path"]

            return jsonify(response_data)

        except Exception as exec_error:
            # Update execution tracking with error
            executions[execution_request.execution_id].update({
                'status': ExecutionStatus.FAILED,
                'completedAt': datetime.now().isoformat(),
                'errors': [str(exec_error)]
            })

            # Return error response
            return jsonify({
                'success': False,
                'executionId': execution_request.execution_id,
                'results': [],
                'logs': [f"Pipeline execution failed: {str(exec_error)}"],
                'errors': [str(exec_error)],
                'completedAt': datetime.now().isoformat()
            })

    except Exception as e:
        logger.error(f"Pipeline execution error: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'executionId': None,
            'logs': [f"Pipeline execution error: {str(e)}"],
            'errors': [str(e)]
        }), 500


@app.route('/api/pipeline/execution/<execution_id>', methods=['GET'])
def get_execution_status(execution_id: str):
    """Get the status and results of a pipeline execution."""
    try:
        if execution_id not in executions:
            return jsonify({'error': 'Execution not found'}), 404

        execution_data = executions[execution_id]

        return jsonify({
            'executionId': execution_id,
            'status': execution_data['status'],
            'startedAt': execution_data['startedAt'],
            'completedAt': execution_data.get('completedAt'),
            'results': execution_data.get('results', {}),
            'logs': execution_data.get('logs', []),
            'errors': execution_data.get('errors', [])
        })

    except Exception as e:
        logger.error(f"Execution status error: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/export/ocel/<execution_id>', methods=['POST'])
def export_ocel(execution_id: str):
    """Export execution results to OCEL format."""
    try:
        if execution_id not in executions:
            return jsonify({'error': 'Execution not found'}), 404

        execution_data = executions[execution_id]
        if execution_data['status'] != ExecutionStatus.COMPLETED:
            return jsonify({'error': 'Execution not completed successfully'}), 400

        # Get export options
        export_options = request.get_json() or {}
        format_type = export_options.get('format', 'OCEL 2.0 JSON')
        filename = export_options.get('filename', f'export_{execution_id}.ocel')

        # Export CORE model to OCEL
        export_result = ocel_service.export_core_model(
            execution_data['results'].get('core_model'),
            format_type,
            filename
        )

        if export_result['success']:
            # Return file for download
            return send_file(
                export_result['file_path'],
                as_attachment=True,
                download_name=export_result['filename']
            )
        else:
            return jsonify({'error': export_result['error']}), 500

    except Exception as e:
        logger.error(f"OCEL export error: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/node/test', methods=['POST'])
def test_node():
    """Test a single node configuration."""
    try:
        node_data = request.get_json()
        if not node_data:
            return jsonify({'error': 'No node data provided'}), 400

        # Test node configuration
        test_result = pipeline_service.test_node(node_data)

        return jsonify({
            'success': test_result['success'],
            'message': test_result['message'],
            'data': test_result.get('data'),
            'errors': test_result.get('errors', []),
            'testedAt': datetime.now().isoformat()
        })

    except Exception as e:
        logger.error(f"Node test error: {str(e)}")
        return jsonify({
            'success': False,
            'message': str(e),
            'errors': [str(e)]
        }), 500


@app.route('/api/files/list', methods=['GET'])
def list_uploaded_files():
    """List all uploaded files."""
    try:
        files = file_service.list_files()
        return jsonify({'files': files})

    except Exception as e:
        logger.error(f"File list error: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/executions/list', methods=['GET'])
def list_executions():
    """List all pipeline executions."""
    try:
        execution_list = []
        for exec_id, exec_data in executions.items():
            execution_list.append({
                'executionId': exec_id,
                'status': exec_data['status'],
                'startedAt': exec_data['startedAt'],
                'completedAt': exec_data.get('completedAt'),
                'pipelineName': exec_data.get('pipeline', {}).get('name', 'Unnamed Pipeline')
            })

        return jsonify({'executions': execution_list})

    except Exception as e:
        logger.error(f"Executions list error: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.errorhandler(413)
def file_too_large(error):
    """Handle file too large error."""
    return jsonify({'error': 'File too large. Maximum size is 500MB.'}), 413


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors."""
    return jsonify({'error': 'Endpoint not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle internal server errors."""
    logger.error(f"Internal server error: {str(error)}")
    return jsonify({'error': 'Internal server error'}), 500

@app.route('/uploads/<path:filename>', methods=['GET', 'POST'])
def download(filename):
    uploads = os.path.join(current_app.root_path, app.config['UPLOAD_FOLDER'])
    return send_from_directory(uploads, filename)

@app.route('/exports/<path:filename>', methods=['GET', 'POST'])
def download_result(filename):
    uploads = os.path.join(current_app.root_path, app.config['EXPORT_FOLDER'])
    return send_from_directory(uploads, filename)

if __name__ == '__main__':
    # Development server
    app.run(
        host='0.0.0.0',
        port=5100,
        debug=True
    )