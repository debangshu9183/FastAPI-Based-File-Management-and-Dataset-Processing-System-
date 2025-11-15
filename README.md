##FastAPI-Based File Management and Dataset Processing System
Using PostgreSQL, MinIO, and In-Memory Caching
Overview

This project implements a backend system capable of uploading, storing, merging, and processing structured datasets such as CSV and Excel files. It uses FastAPI as the API framework, PostgreSQL for metadata management, MinIO for object-based file storage, and an in-memory caching mechanism for temporary storage of merged datasets. The system enables users to upload files, view stored metadata, merge two datasets based on a join key, preview merged results, and save the merged output back to permanent storage.

Features

Upload CSV or Excel files through a REST API

Store file metadata in PostgreSQL

Store actual file content in MinIO

Retrieve list of stored files with details

Merge two datasets with configurable join options

Temporarily cache merged datasets for faster access

Save merged datasets permanently to MinIO

Delete stored files along with metadata

Architecture

The system consists of four main components:

FastAPI server handling HTTP requests and orchestration

PostgreSQL database storing metadata such as file names, formats, and timestamps

MinIO object storage containing the actual uploaded and merged files

An in-memory caching backend used for temporary merged data

This architecture models industry-standard data engineering workflows that separate metadata, storage, and processing layers.

<img width="643" height="368" alt="Image" src="https://github.com/user-attachments/assets/3bb455a7-f5eb-4dad-b45d-f8024b150039" />

Use Cases

Data engineering pipelines involving repeated merging of datasets

Analytical workflows requiring file uploads and metadata management

Lightweight alternatives to full cloud storage solutions such as AWS S3

Teaching and demonstration of API-based data processing architectures

Project Objectives

Develop REST endpoints for file upload, retrieval, merging, and saving

Maintain rich metadata for each uploaded file

Provide reliable object storage through MinIO

Implement Dataset merging with configurable join types

Introduce a caching layer for temporary, fast-access data

Ensure robustness through validation and error handling

Tools and Technologies

FastAPI

PostgreSQL

MinIO (S3-compatible storage)

Pandas

In-memory caching

Uvicorn

Python environment with supporting libraries

How It Works

Users upload files via the API. Metadata is stored in PostgreSQL and the file is uploaded to MinIO.

Users retrieve a list of available files.

Users request merging of two files by specifying their IDs and join settings.

Files are fetched from MinIO, read into DataFrames, merged, and cached.

Users can then choose to save the merged dataset, which is uploaded to MinIO and registered in PostgreSQL.

Files can be deleted upon request, removing both metadata and stored objects.

Internship Context

This project was developed as part of the IDEAS Internship Program at ISI Kolkata to demonstrate backend engineering concepts, file management workflows, cloud storage systems, and real-world data processing practices.

Future Improvements

Authentication and role-based access

Distributed caching using Redis

File versioning and improved object naming

Automated tests and CI/CD integration

Web dashboard for easier interaction
