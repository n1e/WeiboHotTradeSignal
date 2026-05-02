#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Web可视化应用 - 微博热搜交易信号分析器
"""

import os
import sys
import json
import threading
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any

from flask import Flask, render_template, jsonify, request, send_from_directory

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from duckdb_storage import DuckDBStorage


app = Flask(__name__)

config = None
storage = None
combined_scheduler = None
task_history = []
task_running = False
task_lock = threading.Lock()


def load_config():
    """加载配置文件"""
    global config
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    if not os.path.exists(config_path):
        config_path = os.path.join(os.path.dirname(__file__), 'config.example.json')
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return True
    except Exception as e:
        print(f"加载配置文件失败: {e}")
        return False


def init_storage():
    """初始化数据存储"""
    global storage
    if config:
        storage = DuckDBStorage(config)
        return True
    return False


def get_latest_analysis():
    """获取最新的分析结果"""
    reports_dir = config.get('report', {}).get('output_dir', './reports')
    reports_dir = os.path.join(os.path.dirname(__file__), reports_dir)
    
    if not os.path.exists(reports_dir):
        return None
    
    json_files = []
    for filename in os.listdir(reports_dir):
        if filename.startswith('analysis_') and filename.endswith('.json'):
            filepath = os.path.join(reports_dir, filename)
            json_files.append((filepath, os.path.getmtime(filepath)))
    
    if not json_files:
        return None
    
    json_files.sort(key=lambda x: x[1], reverse=True)
    latest_file = json_files[0][0]
    
    try:
        with open(latest_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"读取分析结果失败: {e}")
        return None


def add_task_history(task_type: str, result: Dict[str, Any]):
    """添加任务历史记录"""
    global task_history
    task_entry = {
        'id': result.get('run_id', f'run_{datetime.now().strftime("%Y%m%d_%H%M%S")}'),
        'type': task_type,
        'start_time': result.get('start_time'),
        'end_time': result.get('end_time'),
        'duration': result.get('duration_seconds', 0),
        'success': result.get('success', False),
        'details': result.get('details', {})
    }
    task_history.insert(0, task_entry)
    if len(task_history) > 100:
        task_history = task_history[:100]


def run_full_task():
    """执行完整任务（数据采集+AI分析+报告生成）"""
    global task_running
    
    with task_lock:
        if task_running:
            return {'success': False, 'error': '任务正在执行中'}
        task_running = True
    
    try:
        from main import run_once
        import argparse
        
        args = argparse.Namespace(
            skip_scrape=False,
            skip_analysis=False,
            skip_report=False,
            skip_push=False
        )
        
        result = run_once(config, args)
        
        add_task_history('full', {
            'run_id': f'run_{datetime.now().strftime("%Y%m%d_%H%M%S")}',
            'start_time': datetime.now().isoformat(),
            'end_time': datetime.now().isoformat(),
            'duration_seconds': 0,
            'success': result.get('success', False),
            'details': {'message': '完整任务执行完成'}
        })
        
        return result
        
    except Exception as e:
        add_task_history('full', {
            'run_id': f'run_{datetime.now().strftime("%Y%m%d_%H%M%S")}',
            'start_time': datetime.now().isoformat(),
            'end_time': datetime.now().isoformat(),
            'duration_seconds': 0,
            'success': False,
            'details': {'error': str(e)}
        })
        return {'success': False, 'error': str(e)}
    finally:
        with task_lock:
            task_running = False


def run_daily_summary_task():
    """执行每日总结任务"""
    global task_running
    
    with task_lock:
        if task_running:
            return {'success': False, 'error': '任务正在执行中'}
        task_running = True
    
    try:
        from main import run_daily_summary
        result = run_daily_summary(config)
        
        add_task_history('daily_summary', {
            'run_id': f'daily_{datetime.now().strftime("%Y%m%d_%H%M%S")}',
            'start_time': datetime.now().isoformat(),
            'end_time': datetime.now().isoformat(),
            'duration_seconds': 0,
            'success': result is not None,
            'details': {'total_topics': result.get('total_topics', 0) if result else 0}
        })
        
        return {'success': result is not None, 'result': result}
        
    except Exception as e:
        add_task_history('daily_summary', {
            'run_id': f'daily_{datetime.now().strftime("%Y%m%d_%H%M%S")}',
            'start_time': datetime.now().isoformat(),
            'end_time': datetime.now().isoformat(),
            'duration_seconds': 0,
            'success': False,
            'details': {'error': str(e)}
        })
        return {'success': False, 'error': str(e)}
    finally:
        with task_lock:
            task_running = False


def run_weekly_summary_task():
    """执行每周总结任务"""
    global task_running
    
    with task_lock:
        if task_running:
            return {'success': False, 'error': '任务正在执行中'}
        task_running = True
    
    try:
        from main import run_weekly_summary
        result = run_weekly_summary(config)
        
        add_task_history('weekly_summary', {
            'run_id': f'weekly_{datetime.now().strftime("%Y%m%d_%H%M%S")}',
            'start_time': datetime.now().isoformat(),
            'end_time': datetime.now().isoformat(),
            'duration_seconds': 0,
            'success': result is not None,
            'details': {'total_topics': result.get('total_topics', 0) if result else 0}
        })
        
        return {'success': result is not None, 'result': result}
        
    except Exception as e:
        add_task_history('weekly_summary', {
            'run_id': f'weekly_{datetime.now().strftime("%Y%m%d_%H%M%S")}',
            'start_time': datetime.now().isoformat(),
            'end_time': datetime.now().isoformat(),
            'duration_seconds': 0,
            'success': False,
            'details': {'error': str(e)}
        })
        return {'success': False, 'error': str(e)}
    finally:
        with task_lock:
            task_running = False


def run_investment_mining_task(target_date_str: str = None):
    """执行投资题材挖掘任务"""
    global task_running
    
    with task_lock:
        if task_running:
            return {'success': False, 'error': '任务正在执行中'}
        task_running = True
    
    try:
        from main import run_investment_mining
        result = run_investment_mining(config, target_date_str)
        
        topics_count = 0
        if result and result.get('analysis_result'):
            topics_count = len(result.get('analysis_result', {}).get('investment_topics', []))
        
        add_task_history('investment_mining', {
            'run_id': f'investment_{datetime.now().strftime("%Y%m%d_%H%M%S")}',
            'start_time': datetime.now().isoformat(),
            'end_time': datetime.now().isoformat(),
            'duration_seconds': 0,
            'success': result is not None,
            'details': {
                'topics_count': topics_count,
                'save_success': result.get('save_success', False) if result else False,
                'push_success': result.get('push_success', False) if result else False
            }
        })
        
        return {'success': result is not None, 'result': result}
        
    except Exception as e:
        add_task_history('investment_mining', {
            'run_id': f'investment_{datetime.now().strftime("%Y%m%d_%H%M%S")}',
            'start_time': datetime.now().isoformat(),
            'end_time': datetime.now().isoformat(),
            'duration_seconds': 0,
            'success': False,
            'details': {'error': str(e)}
        })
        return {'success': False, 'error': str(e)}
    finally:
        with task_lock:
            task_running = False


@app.route('/')
def index():
    """首页"""
    return render_template('web/index.html')


@app.route('/realtime')
def realtime():
    """实时热搜页面"""
    return render_template('web/realtime.html')


@app.route('/intraday')
def intraday():
    """日内变化页面"""
    return render_template('web/intraday.html')


@app.route('/summary')
def summary():
    """总结页面"""
    return render_template('web/summary.html')


@app.route('/tasks')
def tasks():
    """任务管理页面"""
    return render_template('web/tasks.html')


@app.route('/investment-topics')
def investment_topics():
    """投资题材分析页面"""
    return render_template('web/investment_topics.html')


@app.route('/api/latest')
def api_latest():
    """获取最新快照数据"""
    if not storage:
        return jsonify({'error': '数据存储未初始化'}), 500
    
    latest_snapshot = storage.get_latest_snapshot(include_items=True)
    analysis_result = get_latest_analysis()
    
    result = {
        'snapshot': latest_snapshot,
        'analysis': analysis_result,
        'snapshot_count': storage.get_snapshot_count(),
        'item_count': storage.get_item_count()
    }
    
    return jsonify(result)


@app.route('/api/intraday')
def api_intraday():
    """获取日内数据"""
    if not storage:
        return jsonify({'error': '数据存储未初始化'}), 500
    
    date_str = request.args.get('date')
    
    if date_str:
        try:
            target_date = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': '无效的日期格式'}), 400
    else:
        target_date = datetime.now()
    
    start_of_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = target_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    snapshots = storage.get_history_by_time_range(start_of_day, end_of_day, include_items=True)
    
    snapshots.sort(key=lambda x: x.get('timestamp', ''))
    
    all_titles = set()
    for snapshot in snapshots:
        for item in snapshot.get('hot_list', []):
            all_titles.add(item.get('title', ''))
    
    chart_data = []
    for title in all_titles:
        if not title:
            continue
        
        rank_series = []
        time_points = []
        
        for snapshot in snapshots:
            snapshot_time = snapshot.get('timestamp', '')
            try:
                dt = datetime.fromisoformat(snapshot_time)
                time_str = dt.strftime('%H:%M')
            except:
                time_str = snapshot_time
            
            found = False
            for item in snapshot.get('hot_list', []):
                if item.get('title') == title:
                    rank_series.append(item.get('rank', 0))
                    found = True
                    break
            
            if not found:
                rank_series.append(None)
            
            time_points.append(time_str)
        
        valid_ranks = [r for r in rank_series if r is not None]
        if len(valid_ranks) >= 2:
            chart_data.append({
                'title': title,
                'time_points': time_points,
                'rank_series': rank_series
            })
    
    table_data = {
        'time_points': [],
        'title_columns': [],
        'rank_matrix': []
    }
    
    if snapshots:
        for snapshot in snapshots:
            snapshot_time = snapshot.get('timestamp', '')
            try:
                dt = datetime.fromisoformat(snapshot_time)
                time_str = dt.strftime('%H:%M:%S')
            except:
                time_str = snapshot_time
            table_data['time_points'].append(time_str)
        
        title_count = {}
        for snapshot in snapshots:
            for item in snapshot.get('hot_list', []):
                title = item.get('title', '')
                if title:
                    title_count[title] = title_count.get(title, 0) + 1
        
        sorted_titles = sorted(title_count.items(), key=lambda x: x[1], reverse=True)[:30]
        table_data['title_columns'] = [t[0] for t in sorted_titles]
        
        for snapshot in snapshots:
            row = []
            for title in table_data['title_columns']:
                found_rank = None
                for item in snapshot.get('hot_list', []):
                    if item.get('title') == title:
                        found_rank = item.get('rank')
                        break
                row.append(found_rank)
            table_data['rank_matrix'].append(row)
    
    return jsonify({
        'date': target_date.strftime('%Y-%m-%d'),
        'snapshot_count': len(snapshots),
        'chart_data': chart_data,
        'table_data': table_data
    })


@app.route('/api/daily-summary')
def api_daily_summary():
    """获取每日总结"""
    if not storage:
        return jsonify({'error': '数据存储未初始化'}), 500
    
    date_str = request.args.get('date')
    
    if date_str:
        try:
            target_date = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': '无效的日期格式'}), 400
    else:
        target_date = datetime.now() - timedelta(days=1)
    
    summary = storage.get_daily_hot_topic_summary(target_date)
    
    if not summary:
        return jsonify({
            'date': target_date.strftime('%Y-%m-%d'),
            'exists': False,
            'topics': []
        })
    
    return jsonify({
        'date': summary.get('summary_date'),
        'exists': True,
        'total_snapshots': summary.get('total_snapshots'),
        'total_topics': summary.get('total_topics'),
        'summary_text': summary.get('summary_text'),
        'topics': summary.get('topics', [])
    })


@app.route('/api/weekly-summary')
def api_weekly_summary():
    """获取每周总结"""
    if not storage:
        return jsonify({'error': '数据存储未初始化'}), 500
    
    week_start_str = request.args.get('week_start')
    week_end_str = request.args.get('week_end')
    
    if week_start_str and week_end_str:
        try:
            week_start = datetime.strptime(week_start_str, '%Y-%m-%d')
            week_end = datetime.strptime(week_end_str, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': '无效的日期格式'}), 400
    else:
        today = datetime.now()
        weekday = today.weekday()
        week_end = today - timedelta(days=weekday + 1)
        week_start = week_end - timedelta(days=6)
    
    summary = storage.get_weekly_hot_topic_summary(week_start, week_end)
    
    if not summary:
        return jsonify({
            'week_start': week_start.strftime('%Y-%m-%d'),
            'week_end': week_end.strftime('%Y-%m-%d'),
            'exists': False,
            'topics': []
        })
    
    return jsonify({
        'week_start_date': summary.get('week_start_date'),
        'week_end_date': summary.get('week_end_date'),
        'exists': True,
        'total_daily_summaries': summary.get('total_daily_summaries'),
        'total_topics': summary.get('total_topics'),
        'summary_text': summary.get('summary_text'),
        'topics': summary.get('topics', [])
    })


@app.route('/api/daily-summaries')
def api_daily_summaries():
    """获取一段时间内的每日总结列表"""
    if not storage:
        return jsonify({'error': '数据存储未初始化'}), 500
    
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    days = request.args.get('days', 7)
    
    try:
        days = int(days)
    except:
        days = 7
    
    if start_date_str and end_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': '无效的日期格式'}), 400
    else:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days - 1)
    
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    try:
        import duckdb
        
        with duckdb.connect(storage.db_path) as conn:
            summaries = conn.execute("""
                SELECT id, summary_date, total_snapshots, total_topics, summary_text, created_at, updated_at
                FROM daily_hot_topic_summaries
                WHERE summary_date >= ? AND summary_date <= ?
                ORDER BY summary_date DESC
            """, [start_date.date(), end_date.date()]).fetchall()
            
            result = []
            for summary_row in summaries:
                summary_id, summary_date, total_snapshots, total_topics, summary_text, created_at, updated_at = summary_row
                
                topics = conn.execute("""
                    SELECT rank, title, appear_count, best_rank, avg_hot_value, max_hot_value,
                           first_appear_time, last_appear_time, is_persistent, persistence_reason
                    FROM daily_hot_topic_items
                    WHERE daily_summary_id = ?
                    ORDER BY rank
                    LIMIT 20
                """, [summary_id]).fetchall()
                
                topic_list = []
                for row in topics:
                    rank, title, appear_count, best_rank, avg_hot_value, max_hot_value, \
                        first_appear_time, last_appear_time, is_persistent, persistence_reason = row
                    
                    topic_list.append({
                        'rank': rank,
                        'title': title,
                        'appear_count': appear_count,
                        'best_rank': best_rank,
                        'avg_hot_value': avg_hot_value,
                        'max_hot_value': max_hot_value,
                        'first_appear_time': first_appear_time.isoformat() if first_appear_time else None,
                        'last_appear_time': last_appear_time.isoformat() if last_appear_time else None,
                        'is_persistent': is_persistent,
                        'persistence_reason': persistence_reason
                    })
                
                result.append({
                    'id': summary_id,
                    'summary_date': summary_date.isoformat(),
                    'total_snapshots': total_snapshots,
                    'total_topics': total_topics,
                    'summary_text': summary_text,
                    'topics': topic_list,
                    'created_at': created_at.isoformat() if created_at else None,
                    'updated_at': updated_at.isoformat() if updated_at else None
                })
            
            return jsonify({
                'start_date': start_date.strftime('%Y-%m-%d'),
                'end_date': end_date.strftime('%Y-%m-%d'),
                'count': len(result),
                'summaries': result
            })
            
    except Exception as e:
        print(f"获取每日总结列表失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/weekly-summaries')
def api_weekly_summaries():
    """获取一段时间内的每周总结列表"""
    if not storage:
        return jsonify({'error': '数据存储未初始化'}), 500
    
    weeks = request.args.get('weeks', 4)
    
    try:
        weeks = int(weeks)
    except:
        weeks = 4
    
    today = datetime.now()
    
    try:
        import duckdb
        
        with duckdb.connect(storage.db_path) as conn:
            summaries = conn.execute("""
                SELECT id, week_start_date, week_end_date, total_daily_summaries, total_topics, summary_text, created_at, updated_at
                FROM weekly_hot_topic_summaries
                ORDER BY week_start_date DESC
                LIMIT ?
            """, [weeks]).fetchall()
            
            result = []
            for summary_row in summaries:
                summary_id, week_start_date, week_end_date, total_daily_summaries, total_topics, summary_text, created_at, updated_at = summary_row
                
                topics = conn.execute("""
                    SELECT rank, title, appear_days, daily_appear_detail, heat_trend, heat_evolution,
                           first_appear_date, last_appear_date, is_sustained, sustained_reason
                    FROM weekly_hot_topic_items
                    WHERE weekly_summary_id = ?
                    ORDER BY rank
                    LIMIT 20
                """, [summary_id]).fetchall()
                
                topic_list = []
                for row in topics:
                    rank, title, appear_days, daily_appear_detail, heat_trend, heat_evolution, \
                        first_appear_date, last_appear_date, is_sustained, sustained_reason = row
                    
                    daily_detail = {}
                    if daily_appear_detail:
                        try:
                            import json
                            daily_detail = json.loads(daily_appear_detail)
                        except:
                            pass
                    
                    topic_list.append({
                        'rank': rank,
                        'title': title,
                        'appear_days': appear_days,
                        'daily_appear_detail': daily_detail,
                        'heat_trend': heat_trend,
                        'heat_evolution': heat_evolution,
                        'first_appear_date': first_appear_date.isoformat() if first_appear_date else None,
                        'last_appear_date': last_appear_date.isoformat() if last_appear_date else None,
                        'is_sustained': is_sustained,
                        'sustained_reason': sustained_reason
                    })
                
                result.append({
                    'id': summary_id,
                    'week_start_date': week_start_date.isoformat(),
                    'week_end_date': week_end_date.isoformat(),
                    'total_daily_summaries': total_daily_summaries,
                    'total_topics': total_topics,
                    'summary_text': summary_text,
                    'topics': topic_list,
                    'created_at': created_at.isoformat() if created_at else None,
                    'updated_at': updated_at.isoformat() if updated_at else None
                })
            
            return jsonify({
                'count': len(result),
                'summaries': result
            })
            
    except Exception as e:
        print(f"获取每周总结列表失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/task/run', methods=['POST'])
def api_run_task():
    """执行任务"""
    task_type = request.json.get('type', 'full')
    target_date = request.json.get('date')
    
    if task_type == 'full':
        result = run_full_task()
    elif task_type == 'daily_summary':
        result = run_daily_summary_task()
    elif task_type == 'weekly_summary':
        result = run_weekly_summary_task()
    elif task_type == 'investment_mining':
        result = run_investment_mining_task(target_date)
    else:
        return jsonify({'error': '无效的任务类型'}), 400
    
    return jsonify(result)


@app.route('/api/task/status')
def api_task_status():
    """获取任务状态"""
    with task_lock:
        is_running = task_running
    
    return jsonify({
        'running': is_running,
        'history': task_history[:20]
    })


@app.route('/api/investment-topic')
def api_investment_topic():
    """获取单日投资题材分析"""
    if not storage:
        return jsonify({'error': '数据存储未初始化'}), 500
    
    date_str = request.args.get('date')
    
    if date_str:
        try:
            target_date = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': '无效的日期格式'}), 400
    else:
        target_date = datetime.now()
    
    analysis = storage.get_investment_topic_analysis(target_date)
    
    if not analysis:
        return jsonify({
            'date': target_date.strftime('%Y-%m-%d'),
            'exists': False,
            'topics': []
        })
    
    return jsonify({
        'date': analysis.get('analysis_date'),
        'exists': True,
        'analysis_summary': analysis.get('analysis_summary'),
        'total_topics': analysis.get('total_topics'),
        'total_snapshot_records': analysis.get('total_snapshot_records'),
        'total_unique_topics': analysis.get('total_unique_topics'),
        'topics': analysis.get('topics', []),
        'created_at': analysis.get('created_at'),
        'updated_at': analysis.get('updated_at')
    })


@app.route('/api/investment-topics')
def api_investment_topics():
    """获取日期范围内的投资题材分析列表"""
    if not storage:
        return jsonify({'error': '数据存储未初始化'}), 500
    
    start_date_str = request.args.get('start_date')
    end_date_str = request.args.get('end_date')
    days = request.args.get('days', 7)
    include_topics = request.args.get('include_topics', 'true').lower() == 'true'
    
    try:
        days = int(days)
    except:
        days = 7
    
    if start_date_str and end_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': '无效的日期格式'}), 400
    else:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days - 1)
    
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    try:
        analyses = storage.get_investment_topic_analyses_by_date_range(
            start_date, end_date, include_topics=include_topics
        )
        
        return jsonify({
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'count': len(analyses),
            'analyses': analyses
        })
        
    except Exception as e:
        print(f"获取投资题材分析列表失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/investment-topic/export')
def api_export_investment_topic():
    """导出投资题材分析报告"""
    if not storage:
        return jsonify({'error': '数据存储未初始化'}), 500
    
    date_str = request.args.get('date')
    export_format = request.args.get('format', 'json')
    
    if date_str:
        try:
            target_date = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            return jsonify({'error': '无效的日期格式'}), 400
    else:
        target_date = datetime.now()
    
    analysis = storage.get_investment_topic_analysis(target_date)
    
    if not analysis:
        return jsonify({
            'success': False,
            'error': f'没有找到 {target_date.strftime("%Y-%m-%d")} 的投资题材分析'
        })
    
    if export_format == 'json':
        return jsonify({
            'success': True,
            'format': 'json',
            'data': analysis
        })
    else:
        return jsonify({
            'success': False,
            'error': '不支持的导出格式，仅支持 json 格式'
        })


@app.route('/api/snapshots')
def api_snapshots():
    """获取快照列表"""
    if not storage:
        return jsonify({'error': '数据存储未初始化'}), 500
    
    days = request.args.get('days', 7)
    try:
        days = int(days)
    except:
        days = 7
    
    end_time = datetime.now()
    start_time = end_time - timedelta(days=days)
    
    snapshots = storage.get_history_by_time_range(start_time, end_time, include_items=False)
    
    result = []
    for snapshot in snapshots:
        result.append({
            'timestamp': snapshot.get('timestamp'),
            'total_count': snapshot.get('total_count')
        })
    
    return jsonify(result)


@app.route('/static/<path:filename>')
def serve_static(filename):
    """提供静态文件"""
    static_dir = os.path.join(os.path.dirname(__file__), 'static')
    return send_from_directory(static_dir, filename)


def init_app():
    """初始化应用"""
    if not load_config():
        print("警告: 无法加载配置文件")
        return False
    
    if not init_storage():
        print("警告: 无法初始化数据存储")
        return False
    
    return True


def start_scheduler():
    """启动调度器"""
    global combined_scheduler
    
    from scheduler import CombinedScheduler
    from main import create_task_func
    import argparse
    
    args = argparse.Namespace(
        skip_scrape=False,
        skip_analysis=False,
        skip_report=False,
        skip_push=False
    )
    
    task_func = create_task_func(config, args)
    combined_scheduler = CombinedScheduler(config, task_func)
    
    import threading
    scheduler_thread = threading.Thread(target=combined_scheduler.start, daemon=True)
    scheduler_thread.start()
    
    print("调度器已在后台线程启动")


if __name__ == '__main__':
    print("=" * 60)
    print("微博热搜交易信号分析器 - Web可视化")
    print("=" * 60)
    
    if not init_app():
        print("初始化失败，程序退出")
        sys.exit(1)
    
    start_scheduler_flag = '--daemon' in sys.argv
    if start_scheduler_flag:
        start_scheduler()
    
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', '0') == '1'
    
    print(f"\n启动Web服务器: http://localhost:{port}")
    print(f"可用页面:")
    print(f"  - 首页: http://localhost:{port}/")
    print(f"  - 实时热搜: http://localhost:{port}/realtime")
    print(f"  - 日内变化: http://localhost:{port}/intraday")
    print(f"  - 总结资讯: http://localhost:{port}/summary")
    print(f"  - 任务管理: http://localhost:{port}/tasks")
    
    print(f"\n已注册的API路由:")
    for rule in sorted(app.url_map.iter_rules(), key=lambda r: str(r)):
        if '/api/' in str(rule):
            print(f"  - {str(rule)}")
    
    print("\n按 Ctrl+C 停止服务器")
    print("=" * 60)
    
    app.run(host='0.0.0.0', port=port, debug=debug, threaded=True)
