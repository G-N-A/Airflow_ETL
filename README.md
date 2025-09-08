<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>E-commerce Data Platform - Interactive Documentation</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Montserrat:wght@700&display=swap" rel="stylesheet">
    <style>
        body {
            font-family: 'Inter', sans-serif;
            background-color: #1a1a2e;
            color: #e0e0e0;
        }

        .container {
            max-width: 1200px;
            margin: 0 auto;
        }

        .header {
            background: linear-gradient(to right, #24294b, #32385b);
            border-radius: 1rem;
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
        }

        .card {
            background-color: #2e3550;
            border-radius: 1rem;
            box-shadow: 0 4px 10px rgba(0, 0, 0, 0.2);
            backdrop-filter: blur(10px);
            transition: transform 0.2s ease-in-out, box-shadow 0.2s ease-in-out;
        }

        .card:hover {
            transform: translateY(-5px);
            box-shadow: 0 8px 25px rgba(0, 0, 0, 0.4);
        }

        .section-header {
            cursor: pointer;
            user-select: none;
            transition: background-color 0.2s ease-in-out;
        }

        .accordion-content {
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.5s ease-in-out;
        }

        .accordion-item.active .accordion-content {
            max-height: 1000px; /* A larger value to accommodate content */
        }

        .accordion-item.active .arrow-icon {
            transform: rotate(180deg);
        }

        .arrow-icon {
            transition: transform 0.5s ease-in-out;
        }
    </style>
</head>

<body class="p-6">
    <div class="container py-8 px-4 sm:px-6 lg:px-8">
        <header class="header p-8 sm:p-10 mb-12 text-center">
            <h1 class="text-4xl sm:text-5xl font-extrabold text-white mb-4 leading-tight" style="font-family: 'Montserrat', sans-serif;">
                E-commerce Data Platform
            </h1>
            <p class="text-lg sm:text-xl text-slate-300 max-w-2xl mx-auto">
                A comprehensive guide to the architecture, workflows, and technologies behind a modern data engineering ecosystem.
            </p>
        </header>

        <main class="space-y-8">
            <!-- Project Overview Section -->
            <div class="card accordion-item">
                <div class="section-header p-6 sm:p-8 flex justify-between items-center bg-transparent rounded-t-xl hover:bg-slate-700 transition-colors" data-target="overview">
                    <div class="flex items-center space-x-4">
                        <svg class="w-8 h-8 text-cyan-400" fill="currentColor" viewBox="0 0 24 24">
                            <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 14H9V8h2v8zm4 0h-2V8h2v8z"/>
                        </svg>
                        <h2 class="text-xl sm:text-2xl font-semibold text-slate-100">Project Overview</h2>
                    </div>
                    <span class="text-slate-400 font-bold transform transition-transform duration-300 arrow-icon">&#9660;</span>
                </div>
                <div class="accordion-content p-6 sm:p-8">
                    <p class="text-slate-300 mb-6 leading-relaxed">
                        This platform is a comprehensive data engineering solution designed to simulate, process, and analyze e-commerce data. It uses a modern data stack with a focus on **automation, scalability, and maintainability**.
                    </p>
                    <div class="bg-slate-700 p-6 rounded-lg mb-6 shadow-inner">
                        <h3 class="font-bold text-slate-200 text-lg sm:text-xl mb-3">Core Components:</h3>
                        <ul class="list-disc list-inside space-y-2 text-slate-300">
                            <li><strong class="text-cyan-400">Data Simulation:</strong> Generates realistic e-commerce data.</li>
                            <li><strong class="text-emerald-400">ETL Pipeline:</strong> Cleanses and transforms raw data for analysis.</li>
                            <li><strong class="text-indigo-400">Orchestration:</strong> Automates and schedules the entire workflow.</li>
                        </ul>
                    </div>
                    <div class="bg-slate-800 p-8 rounded-lg border border-slate-700">
                        <h3 class="font-semibold text-slate-100 mb-6 text-center text-xl">The Data Flow Pipeline</h3>
                        <div class="flex flex-col sm:flex-row justify-between items-center text-center sm:text-left space-y-8 sm:space-y-0 sm:space-x-8">
                            <div class="flex flex-col items-center flex-1">
                                <span class="bg-sky-500 p-4 rounded-full mb-3 shadow-lg">
                                    <svg class="w-10 h-10 text-white" fill="currentColor" viewBox="0 0 24 24">
                                        <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zM7 13h5.71l-2.12 2.12 1.41 1.41L17.29 12l-5.29-5.65-1.41 1.41L12.71 11H7v2z"></path>
                                    </svg>
                                </span>
                                <h4 class="font-medium text-slate-200 text-lg">1. Ingestion</h4>
                                <p class="text-sm text-slate-400 mt-1">`ecom_db`</p>
                            </div>
                            <span class="text-slate-500 text-3xl font-extrabold hidden sm:block">&rarr;</span>
                            <div class="flex flex-col items-center flex-1">
                                <span class="bg-emerald-500 p-4 rounded-full mb-3 shadow-lg">
                                    <svg class="w-10 h-10 text-white" fill="currentColor" viewBox="0 0 24 24">
                                        <path d="M12 2L2 12h3v8h14v-8h3L12 2zM9 18H7v-6h2v6zm4 0h-2v-6h2v6zm4 0h-2v-6h2v6z"></path>
                                    </svg>
                                </span>
                                <h4 class="font-medium text-slate-200 text-lg">2. ETL & Transform</h4>
                                <p class="text-sm text-slate-400 mt-1">PySpark</p>
                            </div>
                            <span class="text-slate-500 text-3xl font-extrabold hidden sm:block">&rarr;</span>
                            <div class="flex flex-col items-center flex-1">
                                <span class="bg-indigo-500 p-4 rounded-full mb-3 shadow-lg">
                                    <svg class="w-10 h-10 text-white" fill="currentColor" viewBox="0 0 24 24">
                                        <path d="M12 10.9L17 14L12 17.1L7 14L12 10.9zM12 2L2 12l10 10 10-10L12 2zM4 12l8 8 8-8-8-8L4 12z"></path>
                                    </svg>
                                </span>
                                <h4 class="font-medium text-slate-200 text-lg">3. Data Warehouse</h4>
                                <p class="text-sm text-slate-400 mt-1">`ecom_dwh`</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Technology Stack Section -->
            <div class="card accordion-item">
                <div class="section-header p-6 sm:p-8 flex justify-between items-center bg-transparent rounded-t-xl hover:bg-slate-700 transition-colors" data-target="tech-stack">
                    <div class="flex items-center space-x-4">
                        <svg class="w-8 h-8 text-fuchsia-400" fill="currentColor" viewBox="0 0 24 24">
                            <path d="M12 2L2 12h2v10h16v-10h2L12 2zM14 18h-4v-4h4v4z"/>
                        </svg>
                        <h2 class="text-xl sm:text-2xl font-semibold text-slate-100">Technology Stack</h2>
                    </div>
                    <span class="text-slate-400 font-bold transform transition-transform duration-300 arrow-icon">&#9660;</span>
                </div>
                <div class="accordion-content p-6 sm:p-8">
                    <ul class="space-y-6">
                        <li class="p-6 bg-slate-700 rounded-lg shadow-inner">
                            <h3 class="text-lg sm:text-xl font-bold text-slate-200 flex items-center mb-2">
                                <svg class="w-6 h-6 text-amber-400 mr-3" fill="currentColor" viewBox="0 0 24 24"><path d="M22 13h-4v4h-2v-4h-4v-2h4V7h2v4h4v2zm-12 7H4c-1.1 0-2-.9-2-2V4c0-1.1.9-2 2-2h16c1.1 0 2 .9 2 2v8h-2V4H4v14h6v2z"></path></svg>
                                Apache Airflow
                            </h3>
                            <p class="text-slate-300 leading-relaxed">The central nervous system of the platform. It handles **workflow orchestration, scheduling, dependency management**, and provides a rich UI for monitoring every job run. Its role is to ensure the entire data pipeline is automated and fault-tolerant.</p>
                        </li>
                        <li class="p-6 bg-slate-700 rounded-lg shadow-inner">
                            <h3 class="text-lg sm:text-xl font-bold text-slate-200 flex items-center mb-2">
                                <svg class="w-6 h-6 text-emerald-400 mr-3" fill="currentColor" viewBox="0 0 24 24"><path d="M10 20v-6h4v6h5v-8h3L12 3 2 12h3v8z"></path></svg>
                                PySpark
                            </h3>
                            <p class="text-slate-300 leading-relaxed">The powerful engine for **distributed data processing**. PySpark is used for all ETL operations, from extracting raw data to performing complex transformations, joins, and aggregations. It allows the pipeline to scale efficiently to handle large datasets.</p>
                        </li>
                        <li class="p-6 bg-slate-700 rounded-lg shadow-inner">
                            <h3 class="text-lg sm:text-xl font-bold text-slate-200 flex items-center mb-2">
                                <svg class="w-6 h-6 text-sky-400 mr-3" fill="currentColor" viewBox="0 0 24 24"><path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zM7 13h10v-2H7v2z"></path></svg>
                                Microsoft SQL Server
                            </h3>
                            <p class="text-slate-300 leading-relaxed">Serves as a **dual-purpose data store**. It is the transactional database (`ecom_db`) for raw, live data and the analytical data warehouse (`ecom_dwh`) for the final, transformed data, which is optimized for BI and reporting.</p>
                        </li>
                    </ul>
                </div>
            </div>

            <!-- Operations & Scripts Section -->
            <div class="card accordion-item">
                <div class="section-header p-6 sm:p-8 flex justify-between items-center bg-transparent rounded-b-xl hover:bg-slate-700 transition-colors" data-target="operations">
                    <div class="flex items-center space-x-4">
                        <svg class="w-8 h-8 text-indigo-400" fill="currentColor" viewBox="0 0 24 24">
                            <path d="M14 2H6c-1.1 0-2 .9-2 2v16c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V8l-6-6zM12 17l-4-4h3V9h2v4h3l-4 4z"/>
                        </svg>
                        <h2 class="text-xl sm:text-2xl font-semibold text-slate-100">Operations & Scripts</h2>
                    </div>
                    <span class="text-slate-400 font-bold transform transition-transform duration-300 arrow-icon">&#9660;</span>
                </div>
                <div class="accordion-content p-6 sm:p-8">
                    <div class="bg-slate-700 p-6 rounded-lg mb-6 shadow-inner">
                        <h3 class="font-bold text-lg text-slate-200 mb-2">Key Commands</h3>
                        <p class="text-sm text-slate-300 mb-2 leading-relaxed">Run the core scripts using their wrapper files for consistency.</p>
                        <ul class="text-sm font-mono bg-slate-900 text-slate-200 p-4 rounded-lg border border-slate-700 overflow-x-auto space-y-2">
                            <li>`bash scripts/synth/run_synth_stream.sh`</li>
                            <li>`bash scripts/etl/run_etl.sh`</li>
                        </ul>
                    </div>
                    <div class="bg-slate-700 p-6 rounded-lg shadow-inner">
                        <h3 class="font-bold text-lg text-slate-200 mb-2">Airflow Management</h3>
                        <p class="text-sm text-slate-300 mb-2 leading-relaxed">
                            To manage Airflow, ensure your virtual environment is active and `AIRFLOW_HOME` is set.
                        </p>
                        <ul class="text-sm font-mono bg-slate-900 text-slate-200 p-4 rounded-lg border border-slate-700 overflow-x-auto space-y-2">
                            <li>`source airflow_venv/bin/activate`</li>
                            <li>`export AIRFLOW_HOME=<project_path>`</li>
                            <li>`airflow dags unpause <dag_name>`</li>
                        </ul>
                    </div>
                </div>
            </div>
        </main>
    </div>

    <script>
        document.addEventListener('DOMContentLoaded', () => {
            const accordionItems = document.querySelectorAll('.accordion-item');

            accordionItems.forEach(item => {
                const header = item.querySelector('.section-header');
                header.addEventListener('click', () => {
                    item.classList.toggle('active');
                });
            });
        });
    </script>
</body>

</html>
