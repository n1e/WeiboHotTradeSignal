#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DuckDB数据存储模块 - 用于存储和查询微博热搜历史数据
"""

import os
import json
import duckdb
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple


class DuckDBStorage:
    """DuckDB数据存储管理器"""
    
    def __init__(self, config: Dict):
        """
        初始化DuckDB存储管理器
        
        Args:
            config: 配置字典，包含数据库路径等配置
        """
        self.config = config
        self.data_config = config.get('data', {})
        self.storage_dir = self.data_config.get('storage_dir', './data')
        self.db_path = self.data_config.get('db_path', os.path.join(self.storage_dir, 'weibo_hot.duckdb'))
        
        os.makedirs(self.storage_dir, exist_ok=True)
        
        self._init_database()
    
    def _init_database(self):
        """初始化数据库表和索引"""
        with duckdb.connect(self.db_path) as conn:
            conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_snapshot_id START 1")
            conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_item_id START 1")
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS hot_search_snapshots (
                    id INTEGER PRIMARY KEY DEFAULT nextval('seq_snapshot_id'),
                    snapshot_time TIMESTAMP NOT NULL,
                    total_count INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS hot_search_items (
                    id INTEGER PRIMARY KEY DEFAULT nextval('seq_item_id'),
                    snapshot_id INTEGER NOT NULL,
                    rank INTEGER NOT NULL,
                    title VARCHAR NOT NULL,
                    hot VARCHAR,
                    hot_value DOUBLE,
                    url VARCHAR,
                    is_market BOOLEAN DEFAULT FALSE,
                    snapshot_time TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (snapshot_id) REFERENCES hot_search_snapshots(id)
                )
            """)
            
            conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_time ON hot_search_snapshots(snapshot_time)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_items_snapshot_id ON hot_search_items(snapshot_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_items_time ON hot_search_items(snapshot_time)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_items_title ON hot_search_items(title)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_items_rank ON hot_search_items(rank)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_items_time_title ON hot_search_items(snapshot_time, title)")
            
            conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_daily_summary_id START 1")
            conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_daily_topic_id START 1")
            conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_weekly_summary_id START 1")
            conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_weekly_topic_id START 1")
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_hot_topic_summaries (
                    id INTEGER PRIMARY KEY DEFAULT nextval('seq_daily_summary_id'),
                    summary_date DATE NOT NULL UNIQUE,
                    total_snapshots INTEGER NOT NULL DEFAULT 0,
                    total_topics INTEGER NOT NULL DEFAULT 0,
                    summary_text TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_hot_topic_items (
                    id INTEGER PRIMARY KEY DEFAULT nextval('seq_daily_topic_id'),
                    daily_summary_id INTEGER NOT NULL,
                    rank INTEGER NOT NULL,
                    title VARCHAR NOT NULL,
                    appear_count INTEGER NOT NULL DEFAULT 0,
                    best_rank INTEGER,
                    avg_hot_value DOUBLE,
                    max_hot_value DOUBLE,
                    first_appear_time TIMESTAMP,
                    last_appear_time TIMESTAMP,
                    is_persistent BOOLEAN DEFAULT FALSE,
                    persistence_reason VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (daily_summary_id) REFERENCES daily_hot_topic_summaries(id)
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS weekly_hot_topic_summaries (
                    id INTEGER PRIMARY KEY DEFAULT nextval('seq_weekly_summary_id'),
                    week_start_date DATE NOT NULL UNIQUE,
                    week_end_date DATE NOT NULL,
                    total_daily_summaries INTEGER NOT NULL DEFAULT 0,
                    total_topics INTEGER NOT NULL DEFAULT 0,
                    summary_text TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS weekly_hot_topic_items (
                    id INTEGER PRIMARY KEY DEFAULT nextval('seq_weekly_topic_id'),
                    weekly_summary_id INTEGER NOT NULL,
                    rank INTEGER NOT NULL,
                    title VARCHAR NOT NULL,
                    appear_days INTEGER NOT NULL DEFAULT 0,
                    daily_appear_detail VARCHAR,
                    heat_trend VARCHAR,
                    heat_evolution TEXT,
                    first_appear_date DATE,
                    last_appear_date DATE,
                    is_sustained BOOLEAN DEFAULT FALSE,
                    sustained_reason VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (weekly_summary_id) REFERENCES weekly_hot_topic_summaries(id)
                )
            """)
            
            conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_summary_date ON daily_hot_topic_summaries(summary_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_topic_summary_id ON daily_hot_topic_items(daily_summary_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_topic_title ON daily_hot_topic_items(title)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_weekly_summary_week ON weekly_hot_topic_summaries(week_start_date, week_end_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_weekly_topic_summary_id ON weekly_hot_topic_items(weekly_summary_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_weekly_topic_title ON weekly_hot_topic_items(title)")
            
            conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_investment_analysis_id START 1")
            conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_investment_topic_id START 1")
            conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_beneficiary_stock_id START 1")
            conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_topic_hot_title_id START 1")
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS investment_topic_analyses (
                    id INTEGER PRIMARY KEY DEFAULT nextval('seq_investment_analysis_id'),
                    analysis_date DATE NOT NULL UNIQUE,
                    analysis_summary TEXT,
                    total_topics INTEGER NOT NULL DEFAULT 0,
                    total_snapshot_records INTEGER NOT NULL DEFAULT 0,
                    total_unique_topics INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS investment_topics (
                    id INTEGER PRIMARY KEY DEFAULT nextval('seq_investment_topic_id'),
                    analysis_id INTEGER NOT NULL,
                    rank INTEGER NOT NULL,
                    topic_name VARCHAR NOT NULL,
                    analysis_dimension VARCHAR,
                    confidence_level VARCHAR,
                    core_logic TEXT,
                    market_expectation TEXT,
                    related_industries_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (analysis_id) REFERENCES investment_topic_analyses(id)
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS beneficiary_stocks (
                    id INTEGER PRIMARY KEY DEFAULT nextval('seq_beneficiary_stock_id'),
                    topic_id INTEGER NOT NULL,
                    stock_name VARCHAR NOT NULL,
                    stock_code VARCHAR,
                    benefit_reason TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (topic_id) REFERENCES investment_topics(id)
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS topic_related_hot_titles (
                    id INTEGER PRIMARY KEY DEFAULT nextval('seq_topic_hot_title_id'),
                    topic_id INTEGER NOT NULL,
                    hot_title VARCHAR NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (topic_id) REFERENCES investment_topics(id)
                )
            """)
            
            conn.execute("CREATE INDEX IF NOT EXISTS idx_investment_analysis_date ON investment_topic_analyses(analysis_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_investment_topic_analysis_id ON investment_topics(analysis_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_investment_topic_name ON investment_topics(topic_name)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_beneficiary_stock_topic_id ON beneficiary_stocks(topic_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_topic_hot_title_topic_id ON topic_related_hot_titles(topic_id)")
            
            conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_alert_config_id START 1")
            conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_alert_keyword_id START 1")
            conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_alert_event_id START 1")
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS alert_configs (
                    id INTEGER PRIMARY KEY DEFAULT nextval('seq_alert_config_id'),
                    config_name VARCHAR NOT NULL DEFAULT 'default',
                    enabled BOOLEAN DEFAULT TRUE,
                    new_topic_enabled BOOLEAN DEFAULT TRUE,
                    new_topic_top_rank_threshold INTEGER DEFAULT 10,
                    rank_surge_enabled BOOLEAN DEFAULT TRUE,
                    rank_surge_threshold INTEGER DEFAULT 30,
                    rank_surge_target_rank INTEGER DEFAULT 10,
                    heat_surge_enabled BOOLEAN DEFAULT TRUE,
                    heat_surge_ratio_threshold DOUBLE DEFAULT 2.0,
                    sudden_disappear_enabled BOOLEAN DEFAULT TRUE,
                    sudden_disappear_days_threshold INTEGER DEFAULT 3,
                    rank_plunge_enabled BOOLEAN DEFAULT TRUE,
                    rank_plunge_threshold INTEGER DEFAULT 20,
                    rank_plunge_start_rank INTEGER DEFAULT 10,
                    alert_level_normal_enabled BOOLEAN DEFAULT TRUE,
                    alert_level_important_enabled BOOLEAN DEFAULT TRUE,
                    alert_level_urgent_enabled BOOLEAN DEFAULT TRUE,
                    normal_push_interval_minutes INTEGER DEFAULT 60,
                    important_push_interval_minutes INTEGER DEFAULT 30,
                    urgent_push_interval_minutes INTEGER DEFAULT 5,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS alert_keywords (
                    id INTEGER PRIMARY KEY DEFAULT nextval('seq_alert_keyword_id'),
                    keyword VARCHAR NOT NULL UNIQUE,
                    is_included BOOLEAN DEFAULT TRUE,
                    priority INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS alert_events (
                    id INTEGER PRIMARY KEY DEFAULT nextval('seq_alert_event_id'),
                    alert_type VARCHAR NOT NULL,
                    alert_level VARCHAR NOT NULL DEFAULT 'normal',
                    title VARCHAR NOT NULL,
                    rank_before INTEGER,
                    rank_after INTEGER,
                    rank_change INTEGER,
                    hot_value_before DOUBLE,
                    hot_value_after DOUBLE,
                    heat_change_ratio DOUBLE,
                    snapshot_time_before TIMESTAMP,
                    snapshot_time_after TIMESTAMP,
                    details TEXT,
                    is_pushed BOOLEAN DEFAULT FALSE,
                    pushed_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_config_name ON alert_configs(config_name)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_keyword ON alert_keywords(keyword)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_event_type ON alert_events(alert_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_event_level ON alert_events(alert_level)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_event_time ON alert_events(created_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_event_title ON alert_events(title)")
            
            conn.execute("""
                INSERT INTO alert_configs (config_name)
                SELECT 'default'
                WHERE NOT EXISTS (SELECT 1 FROM alert_configs WHERE config_name = 'default')
            """)
            
            conn.commit()
    
    def _parse_hot_value(self, hot_str: str) -> float:
        """
        解析热度值，将"500万"转换为数值
        
        Args:
            hot_str: 热度字符串，如 "500万"、"300万" 等
            
        Returns:
            转换后的数值，如 5000000.0
        """
        if not hot_str:
            return 0.0
        
        hot_str = hot_str.strip()
        
        if '万' in hot_str:
            try:
                num = float(hot_str.replace('万', '').strip())
                return num * 10000
            except ValueError:
                return 0.0
        else:
            try:
                return float(hot_str)
            except ValueError:
                return 0.0
    
    def save_hot_search(self, data: Dict) -> Optional[int]:
        """
        保存热搜数据到数据库
        
        Args:
            data: 热搜数据字典，格式如下：
                {
                    'timestamp': '2026-04-21T10:00:00',
                    'total_count': 50,
                    'hot_list': [
                        {'rank': 1, 'title': '...', 'hot': '500万', 'url': '...', 'is_market': False},
                        ...
                    ]
                }
        
        Returns:
            快照ID，如果保存失败返回None
        """
        if not data:
            print("没有数据可保存")
            return None
        
        try:
            timestamp_str = data.get('timestamp', datetime.now().isoformat())
            snapshot_time = datetime.fromisoformat(timestamp_str)
            total_count = data.get('total_count', 0)
            hot_list = data.get('hot_list', [])
            
            with duckdb.connect(self.db_path) as conn:
                result = conn.execute("""
                    INSERT INTO hot_search_snapshots (snapshot_time, total_count, created_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                    RETURNING id
                """, [snapshot_time, total_count]).fetchone()
                
                if not result:
                    print("保存快照失败")
                    return None
                
                snapshot_id = result[0]
                
                for item in hot_list:
                    rank = item.get('rank', 0)
                    title = item.get('title', '')
                    hot = item.get('hot', '')
                    url = item.get('url', '')
                    is_market = item.get('is_market', False)
                    hot_value = self._parse_hot_value(hot)
                    
                    conn.execute("""
                        INSERT INTO hot_search_items 
                        (snapshot_id, rank, title, hot, hot_value, url, is_market, snapshot_time, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, [snapshot_id, rank, title, hot, hot_value, url, is_market, snapshot_time])
                
                conn.commit()
                
                print(f"数据已保存到数据库: 快照ID={snapshot_id}, 热搜数量={len(hot_list)}")
                return snapshot_id
                
        except Exception as e:
            print(f"保存数据到数据库失败: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def get_history_by_time_range(
        self, 
        start_time: datetime, 
        end_time: datetime,
        include_items: bool = True
    ) -> List[Dict]:
        """
        按时间范围查询历史数据
        
        Args:
            start_time: 开始时间
            end_time: 结束时间
            include_items: 是否包含热搜详情项
        
        Returns:
            历史数据列表，格式与save_hot_search的输入格式一致
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                snapshots = conn.execute("""
                    SELECT id, snapshot_time, total_count
                    FROM hot_search_snapshots
                    WHERE snapshot_time >= ? AND snapshot_time <= ?
                    ORDER BY snapshot_time DESC
                """, [start_time, end_time]).fetchall()
                
                result = []
                
                for snapshot in snapshots:
                    snapshot_id, snapshot_time, total_count = snapshot
                    
                    snapshot_data = {
                        'timestamp': snapshot_time.isoformat(),
                        'total_count': total_count,
                        'hot_list': []
                    }
                    
                    if include_items:
                        items = conn.execute("""
                            SELECT rank, title, hot, hot_value, url, is_market
                            FROM hot_search_items
                            WHERE snapshot_id = ?
                            ORDER BY rank
                        """, [snapshot_id]).fetchall()
                        
                        for item in items:
                            rank, title, hot, hot_value, url, is_market = item
                            snapshot_data['hot_list'].append({
                                'rank': rank,
                                'title': title,
                                'hot': hot,
                                'hot_value': hot_value,
                                'url': url,
                                'is_market': is_market
                            })
                    
                    result.append(snapshot_data)
                
                return result
                
        except Exception as e:
            print(f"按时间范围查询历史数据失败: {e}")
            return []
    
    def get_history_by_days(self, days: int = 7, include_items: bool = True) -> List[Dict]:
        """
        获取最近N天的历史数据
        
        Args:
            days: 天数
            include_items: 是否包含热搜详情项
        
        Returns:
            历史数据列表
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)
        
        return self.get_history_by_time_range(start_time, end_time, include_items)
    
    def get_latest_snapshot(self, include_items: bool = True) -> Optional[Dict]:
        """
        获取最新的快照数据
        
        Args:
            include_items: 是否包含热搜详情项
        
        Returns:
            最新的快照数据，如果没有则返回None
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                snapshot = conn.execute("""
                    SELECT id, snapshot_time, total_count
                    FROM hot_search_snapshots
                    ORDER BY snapshot_time DESC
                    LIMIT 1
                """).fetchone()
                
                if not snapshot:
                    return None
                
                snapshot_id, snapshot_time, total_count = snapshot
                
                result = {
                    'timestamp': snapshot_time.isoformat(),
                    'total_count': total_count,
                    'hot_list': []
                }
                
                if include_items:
                    items = conn.execute("""
                        SELECT rank, title, hot, hot_value, url, is_market
                        FROM hot_search_items
                        WHERE snapshot_id = ?
                        ORDER BY rank
                    """, [snapshot_id]).fetchall()
                    
                    for item in items:
                        rank, title, hot, hot_value, url, is_market = item
                        result['hot_list'].append({
                            'rank': rank,
                            'title': title,
                            'hot': hot,
                            'hot_value': hot_value,
                            'url': url,
                            'is_market': is_market
                        })
                
                return result
                
        except Exception as e:
            print(f"获取最新快照失败: {e}")
            return None
    
    def search_by_title(self, keyword: str, limit: int = 100) -> List[Dict]:
        """
        根据标题关键词搜索热搜记录
        
        Args:
            keyword: 关键词
            limit: 返回结果数量限制
        
        Returns:
            匹配的热搜记录列表
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                results = conn.execute("""
                    SELECT 
                        i.snapshot_time,
                        i.rank,
                        i.title,
                        i.hot,
                        i.hot_value,
                        i.url,
                        i.is_market,
                        s.id as snapshot_id
                    FROM hot_search_items i
                    JOIN hot_search_snapshots s ON i.snapshot_id = s.id
                    WHERE i.title LIKE ?
                    ORDER BY i.snapshot_time DESC, i.rank
                    LIMIT ?
                """, [f'%{keyword}%', limit]).fetchall()
                
                return [
                    {
                        'snapshot_time': row[0].isoformat() if row[0] else None,
                        'rank': row[1],
                        'title': row[2],
                        'hot': row[3],
                        'hot_value': row[4],
                        'url': row[5],
                        'is_market': row[6],
                        'snapshot_id': row[7]
                    }
                    for row in results
                ]
                
        except Exception as e:
            print(f"按标题搜索失败: {e}")
            return []
    
    def get_top_rank_history(self, rank: int = 1, days: int = 30) -> List[Dict]:
        """
        获取特定排名的历史记录
        
        Args:
            rank: 排名
            days: 天数范围
        
        Returns:
            该排名的历史记录
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days)
        
        try:
            with duckdb.connect(self.db_path) as conn:
                results = conn.execute("""
                    SELECT 
                        i.snapshot_time,
                        i.rank,
                        i.title,
                        i.hot,
                        i.hot_value,
                        i.url
                    FROM hot_search_items i
                    WHERE i.rank = ?
                      AND i.snapshot_time >= ?
                      AND i.snapshot_time <= ?
                    ORDER BY i.snapshot_time DESC
                """, [rank, start_time, end_time]).fetchall()
                
                return [
                    {
                        'snapshot_time': row[0].isoformat() if row[0] else None,
                        'rank': row[1],
                        'title': row[2],
                        'hot': row[3],
                        'hot_value': row[4],
                        'url': row[5]
                    }
                    for row in results
                ]
                
        except Exception as e:
            print(f"获取排名历史失败: {e}")
            return []
    
    def get_snapshot_count(self) -> int:
        """
        获取数据库中的快照总数
        
        Returns:
            快照数量
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                result = conn.execute("SELECT COUNT(*) FROM hot_search_snapshots").fetchone()
                return result[0] if result else 0
        except Exception as e:
            print(f"获取快照数量失败: {e}")
            return 0
    
    def get_item_count(self) -> int:
        """
        获取数据库中的热搜项总数
        
        Returns:
            热搜项数量
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                result = conn.execute("SELECT COUNT(*) FROM hot_search_items").fetchone()
                return result[0] if result else 0
        except Exception as e:
            print(f"获取热搜项数量失败: {e}")
            return 0
    
    def migrate_from_json(self, json_dir: str = None) -> Tuple[int, int]:
        """
        从JSON文件迁移数据到数据库
        
        Args:
            json_dir: JSON文件目录，默认为配置的storage_dir
        
        Returns:
            (成功迁移的快照数, 成功迁移的热搜项数)
        """
        if json_dir is None:
            json_dir = self.storage_dir
        
        if not os.path.exists(json_dir):
            print(f"JSON目录不存在: {json_dir}")
            return (0, 0)
        
        json_files = []
        for filename in os.listdir(json_dir):
            if filename.startswith('weibo_hot_') and filename.endswith('.json'):
                filepath = os.path.join(json_dir, filename)
                json_files.append(filepath)
        
        if not json_files:
            print("没有找到可迁移的JSON文件")
            return (0, 0)
        
        json_files.sort(key=lambda x: os.path.getmtime(x))
        
        success_snapshots = 0
        success_items = 0
        
        for filepath in json_files:
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                snapshot_id = self.save_hot_search(data)
                if snapshot_id:
                    success_snapshots += 1
                    success_items += len(data.get('hot_list', []))
                    print(f"已迁移: {filepath}")
                    
            except Exception as e:
                print(f"迁移文件失败 {filepath}: {e}")
                continue
        
        print(f"\n迁移完成: 成功迁移 {success_snapshots} 个快照, {success_items} 条热搜项")
        return (success_snapshots, success_items)
    
    def export_to_json(self, output_dir: str = None, days: int = None) -> int:
        """
        导出数据到JSON文件（用于备份）
        
        Args:
            output_dir: 输出目录
            days: 导出最近N天的数据，None表示导出所有
        
        Returns:
            导出的快照数量
        """
        if output_dir is None:
            output_dir = self.storage_dir
        
        os.makedirs(output_dir, exist_ok=True)
        
        if days:
            history_data = self.get_history_by_days(days, include_items=True)
        else:
            end_time = datetime.now()
            start_time = datetime(2000, 1, 1)
            history_data = self.get_history_by_time_range(start_time, end_time, include_items=True)
        
        export_count = 0
        
        for data in history_data:
            try:
                timestamp = datetime.fromisoformat(data['timestamp'])
                filename = f'weibo_hot_{timestamp.strftime("%Y%m%d_%H%M%S")}.json'
                filepath = os.path.join(output_dir, filename)
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                
                export_count += 1
                
            except Exception as e:
                print(f"导出失败: {e}")
                continue
        
        print(f"导出完成: 导出 {export_count} 个快照到 {output_dir}")
        return export_count
    
    def get_daily_snapshots_for_summary(self, target_date: datetime = None) -> List[Dict]:
        """
        获取某日用于总结的代表性快照数据
        为节省token，只返回代表性快照（第一个、最后一个、中间几个，不超过5个）的关键信息
        
        Args:
            target_date: 目标日期，默认为今天
            
        Returns:
            代表性快照列表，每个快照包含时间和TOP热搜信息
        """
        if target_date is None:
            target_date = datetime.now()
        
        start_of_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = target_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        try:
            with duckdb.connect(self.db_path) as conn:
                snapshots = conn.execute("""
                    SELECT id, snapshot_time, total_count
                    FROM hot_search_snapshots
                    WHERE snapshot_time >= ? AND snapshot_time <= ?
                    ORDER BY snapshot_time
                """, [start_of_day, end_of_day]).fetchall()
                
                if not snapshots:
                    return []
                
                total_snapshots = len(snapshots)
                
                selected_indices = []
                if total_snapshots <= 5:
                    selected_indices = list(range(total_snapshots))
                else:
                    selected_indices = [0, total_snapshots - 1]
                    if total_snapshots >= 3:
                        selected_indices.append(total_snapshots // 2)
                    if total_snapshots >= 4:
                        selected_indices.append(total_snapshots // 3)
                    if total_snapshots >= 5:
                        selected_indices.append(total_snapshots * 2 // 3)
                    selected_indices = sorted(list(set(selected_indices)))
                
                result = []
                for idx in selected_indices:
                    snapshot_id, snapshot_time, total_count = snapshots[idx]
                    
                    items = conn.execute("""
                        SELECT rank, title, hot, hot_value
                        FROM hot_search_items
                        WHERE snapshot_id = ?
                        ORDER BY rank
                        LIMIT 20
                    """, [snapshot_id]).fetchall()
                    
                    hot_list = []
                    for item in items:
                        rank, title, hot, hot_value = item
                        hot_list.append({
                            'rank': rank,
                            'title': title,
                            'hot': hot,
                            'hot_value': hot_value
                        })
                    
                    result.append({
                        'snapshot_time': snapshot_time.isoformat(),
                        'total_count': total_count,
                        'hot_list': hot_list,
                        'is_representative': True
                    })
                
                return result
                
        except Exception as e:
            print(f"获取每日总结快照失败: {e}")
            return []
    
    def get_topic_appearances_by_date(self, target_date: datetime = None) -> Dict[str, Dict]:
        """
        获取某日每个话题的出现统计
        
        Args:
            target_date: 目标日期，默认为今天
            
        Returns:
            字典，key为话题标题，value为统计信息
        """
        if target_date is None:
            target_date = datetime.now()
        
        start_of_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = target_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        try:
            with duckdb.connect(self.db_path) as conn:
                results = conn.execute("""
                    SELECT 
                        title,
                        COUNT(*) as appear_count,
                        MIN(rank) as best_rank,
                        AVG(hot_value) as avg_hot_value,
                        MAX(hot_value) as max_hot_value,
                        MIN(snapshot_time) as first_appear_time,
                        MAX(snapshot_time) as last_appear_time
                    FROM hot_search_items
                    WHERE snapshot_time >= ? AND snapshot_time <= ?
                    GROUP BY title
                    ORDER BY appear_count DESC, best_rank ASC
                """, [start_of_day, end_of_day]).fetchall()
                
                topic_stats = {}
                for row in results:
                    title, appear_count, best_rank, avg_hot_value, max_hot_value, first_appear_time, last_appear_time = row
                    topic_stats[title] = {
                        'appear_count': appear_count,
                        'best_rank': best_rank,
                        'avg_hot_value': avg_hot_value,
                        'max_hot_value': max_hot_value,
                        'first_appear_time': first_appear_time.isoformat() if first_appear_time else None,
                        'last_appear_time': last_appear_time.isoformat() if last_appear_time else None
                    }
                
                return topic_stats
                
        except Exception as e:
            print(f"获取话题统计失败: {e}")
            return {}
    
    def save_daily_hot_topic_summary(
        self,
        summary_date: datetime,
        topics: List[Dict],
        summary_text: str = None
    ) -> Optional[int]:
        """
        保存每日热门话题总结结果
        
        Args:
            summary_date: 总结日期
            topics: 热门话题列表，每个话题包含:
                - rank: 排名
                - title: 标题
                - appear_count: 出现次数
                - best_rank: 最好排名
                - avg_hot_value: 平均热度
                - max_hot_value: 最高热度
                - first_appear_time: 首次出现时间
                - last_appear_time: 最后出现时间
                - is_persistent: 是否为持久性话题
                - persistence_reason: 持久性原因
            summary_text: AI生成的总结文本
            
        Returns:
            总结记录ID，如果保存失败返回None
        """
        try:
            summary_date_only = summary_date.date()
            
            with duckdb.connect(self.db_path) as conn:
                existing = conn.execute("""
                    SELECT id FROM daily_hot_topic_summaries WHERE summary_date = ?
                """, [summary_date_only]).fetchone()
                
                if existing:
                    summary_id = existing[0]
                    conn.execute("""
                        DELETE FROM daily_hot_topic_items WHERE daily_summary_id = ?
                    """, [summary_id])
                    
                    conn.execute("""
                        UPDATE daily_hot_topic_summaries 
                        SET total_topics = ?, summary_text = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, [len(topics), summary_text, summary_id])
                else:
                    result = conn.execute("""
                        INSERT INTO daily_hot_topic_summaries 
                        (summary_date, total_snapshots, total_topics, summary_text, created_at, updated_at)
                        VALUES (?, 0, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        RETURNING id
                    """, [summary_date_only, len(topics), summary_text]).fetchone()
                    
                    if not result:
                        print("保存每日总结失败")
                        return None
                    
                    summary_id = result[0]
                
                for topic in topics:
                    conn.execute("""
                        INSERT INTO daily_hot_topic_items 
                        (daily_summary_id, rank, title, appear_count, best_rank, 
                         avg_hot_value, max_hot_value, first_appear_time, last_appear_time,
                         is_persistent, persistence_reason, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, [
                        summary_id,
                        topic.get('rank', 0),
                        topic.get('title', ''),
                        topic.get('appear_count', 0),
                        topic.get('best_rank'),
                        topic.get('avg_hot_value'),
                        topic.get('max_hot_value'),
                        datetime.fromisoformat(topic['first_appear_time']) if topic.get('first_appear_time') else None,
                        datetime.fromisoformat(topic['last_appear_time']) if topic.get('last_appear_time') else None,
                        topic.get('is_persistent', False),
                        topic.get('persistence_reason')
                    ])
                
                conn.commit()
                
                print(f"每日热门话题总结已保存: 日期={summary_date_only}, 话题数={len(topics)}")
                return summary_id
                
        except Exception as e:
            print(f"保存每日热门话题总结失败: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def get_daily_hot_topic_summary(self, summary_date: datetime) -> Optional[Dict]:
        """
        获取指定日期的热门话题总结
        
        Args:
            summary_date: 总结日期
            
        Returns:
            总结数据字典，如果不存在返回None
        """
        try:
            summary_date_only = summary_date.date()
            
            with duckdb.connect(self.db_path) as conn:
                summary = conn.execute("""
                    SELECT id, summary_date, total_snapshots, total_topics, summary_text, created_at, updated_at
                    FROM daily_hot_topic_summaries
                    WHERE summary_date = ?
                """, [summary_date_only]).fetchone()
                
                if not summary:
                    return None
                
                summary_id, date, total_snapshots, total_topics, summary_text, created_at, updated_at = summary
                
                topics = conn.execute("""
                    SELECT rank, title, appear_count, best_rank, avg_hot_value, max_hot_value,
                           first_appear_time, last_appear_time, is_persistent, persistence_reason
                    FROM daily_hot_topic_items
                    WHERE daily_summary_id = ?
                    ORDER BY rank
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
                
                return {
                    'id': summary_id,
                    'summary_date': date.isoformat(),
                    'total_snapshots': total_snapshots,
                    'total_topics': total_topics,
                    'summary_text': summary_text,
                    'topics': topic_list,
                    'created_at': created_at.isoformat() if created_at else None,
                    'updated_at': updated_at.isoformat() if updated_at else None
                }
                
        except Exception as e:
            print(f"获取每日热门话题总结失败: {e}")
            return None
    
    def get_daily_summaries_for_week(self, week_start: datetime, week_end: datetime) -> List[Dict]:
        """
        获取一周内的每日总结，用于生成每周总结
        
        Args:
            week_start: 周开始日期
            week_end: 周结束日期
            
        Returns:
            每日总结列表
        """
        try:
            start_date = week_start.date()
            end_date = week_end.date()
            
            with duckdb.connect(self.db_path) as conn:
                summaries = conn.execute("""
                    SELECT id, summary_date, total_snapshots, total_topics, summary_text
                    FROM daily_hot_topic_summaries
                    WHERE summary_date >= ? AND summary_date <= ?
                    ORDER BY summary_date
                """, [start_date, end_date]).fetchall()
                
                result = []
                for summary_row in summaries:
                    summary_id, summary_date, total_snapshots, total_topics, summary_text = summary_row
                    
                    topics = conn.execute("""
                        SELECT rank, title, appear_count, best_rank, avg_hot_value, max_hot_value,
                               first_appear_time, last_appear_time, is_persistent, persistence_reason
                        FROM daily_hot_topic_items
                        WHERE daily_summary_id = ?
                        ORDER BY rank
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
                        'summary_date': summary_date.isoformat(),
                        'total_topics': total_topics,
                        'summary_text': summary_text,
                        'topics': topic_list
                    })
                
                return result
                
        except Exception as e:
            print(f"获取一周每日总结失败: {e}")
            return []
    
    def save_weekly_hot_topic_summary(
        self,
        week_start: datetime,
        week_end: datetime,
        topics: List[Dict],
        summary_text: str = None
    ) -> Optional[int]:
        """
        保存每周热门话题总结结果
        
        Args:
            week_start: 周开始日期
            week_end: 周结束日期
            topics: 热门话题列表，每个话题包含:
                - rank: 排名
                - title: 标题
                - appear_days: 出现天数
                - daily_appear_detail: 每日出现详情
                - heat_trend: 热度趋势
                - heat_evolution: 热度演变分析
                - first_appear_date: 首次出现日期
                - last_appear_date: 最后出现日期
                - is_sustained: 是否为持续性话题
                - sustained_reason: 持续性原因
            summary_text: AI生成的总结文本
            
        Returns:
            总结记录ID，如果保存失败返回None
        """
        try:
            week_start_date = week_start.date()
            week_end_date = week_end.date()
            
            with duckdb.connect(self.db_path) as conn:
                existing = conn.execute("""
                    SELECT id FROM weekly_hot_topic_summaries WHERE week_start_date = ? AND week_end_date = ?
                """, [week_start_date, week_end_date]).fetchone()
                
                if existing:
                    summary_id = existing[0]
                    conn.execute("""
                        DELETE FROM weekly_hot_topic_items WHERE weekly_summary_id = ?
                    """, [summary_id])
                    
                    conn.execute("""
                        UPDATE weekly_hot_topic_summaries 
                        SET total_topics = ?, summary_text = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, [len(topics), summary_text, summary_id])
                else:
                    daily_count = conn.execute("""
                        SELECT COUNT(*) FROM daily_hot_topic_summaries
                        WHERE summary_date >= ? AND summary_date <= ?
                    """, [week_start_date, week_end_date]).fetchone()[0]
                    
                    result = conn.execute("""
                        INSERT INTO weekly_hot_topic_summaries 
                        (week_start_date, week_end_date, total_daily_summaries, total_topics, summary_text, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        RETURNING id
                    """, [week_start_date, week_end_date, daily_count, len(topics), summary_text]).fetchone()
                    
                    if not result:
                        print("保存每周总结失败")
                        return None
                    
                    summary_id = result[0]
                
                for topic in topics:
                    daily_appear_detail_json = json.dumps(topic.get('daily_appear_detail', {}), ensure_ascii=False) if topic.get('daily_appear_detail') else None
                    
                    conn.execute("""
                        INSERT INTO weekly_hot_topic_items 
                        (weekly_summary_id, rank, title, appear_days, daily_appear_detail,
                         heat_trend, heat_evolution, first_appear_date, last_appear_date,
                         is_sustained, sustained_reason, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, [
                        summary_id,
                        topic.get('rank', 0),
                        topic.get('title', ''),
                        topic.get('appear_days', 0),
                        daily_appear_detail_json,
                        topic.get('heat_trend'),
                        topic.get('heat_evolution'),
                        topic.get('first_appear_date'),
                        topic.get('last_appear_date'),
                        topic.get('is_sustained', False),
                        topic.get('sustained_reason')
                    ])
                
                conn.commit()
                
                print(f"每周热门话题总结已保存: 周期={week_start_date} 到 {week_end_date}, 话题数={len(topics)}")
                return summary_id
                
        except Exception as e:
            print(f"保存每周热门话题总结失败: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def get_weekly_hot_topic_summary(self, week_start: datetime, week_end: datetime) -> Optional[Dict]:
        """
        获取指定周期的每周热门话题总结
        
        Args:
            week_start: 周开始日期
            week_end: 周结束日期
            
        Returns:
            总结数据字典，如果不存在返回None
        """
        try:
            week_start_date = week_start.date()
            week_end_date = week_end.date()
            
            with duckdb.connect(self.db_path) as conn:
                summary = conn.execute("""
                    SELECT id, week_start_date, week_end_date, total_daily_summaries, total_topics, summary_text, created_at, updated_at
                    FROM weekly_hot_topic_summaries
                    WHERE week_start_date = ? AND week_end_date = ?
                """, [week_start_date, week_end_date]).fetchone()
                
                if not summary:
                    return None
                
                summary_id, start_date, end_date, total_daily_summaries, total_topics, summary_text, created_at, updated_at = summary
                
                topics = conn.execute("""
                    SELECT rank, title, appear_days, daily_appear_detail, heat_trend, heat_evolution,
                           first_appear_date, last_appear_date, is_sustained, sustained_reason
                    FROM weekly_hot_topic_items
                    WHERE weekly_summary_id = ?
                    ORDER BY rank
                """, [summary_id]).fetchall()
                
                topic_list = []
                for row in topics:
                    rank, title, appear_days, daily_appear_detail, heat_trend, heat_evolution, \
                        first_appear_date, last_appear_date, is_sustained, sustained_reason = row
                    
                    daily_detail = {}
                    if daily_appear_detail:
                        try:
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
                
                return {
                    'id': summary_id,
                    'week_start_date': start_date.isoformat(),
                    'week_end_date': end_date.isoformat(),
                    'total_daily_summaries': total_daily_summaries,
                    'total_topics': total_topics,
                    'summary_text': summary_text,
                    'topics': topic_list,
                    'created_at': created_at.isoformat() if created_at else None,
                    'updated_at': updated_at.isoformat() if updated_at else None
                }
                
        except Exception as e:
            print(f"获取每周热门话题总结失败: {e}")
            return None
    
    def clear_daily_hot_topics(self) -> bool:
        """
        清空每日热门话题总结相关的数据表
        
        Returns:
            是否成功
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                conn.execute("DELETE FROM daily_hot_topic_items")
                conn.execute("DELETE FROM daily_hot_topic_summaries")
                conn.commit()
                
                print("已清空每日热门话题总结相关数据表")
                return True
                
        except Exception as e:
            print(f"清空每日热门话题总结数据表失败: {e}")
            return False
    
    def clear_weekly_hot_topics(self) -> bool:
        """
        清空每周热门话题总结相关的数据表
        
        Returns:
            是否成功
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                conn.execute("DELETE FROM weekly_hot_topic_items")
                conn.execute("DELETE FROM weekly_hot_topic_summaries")
                conn.commit()
                
                print("已清空每周热门话题总结相关数据表")
                return True
                
        except Exception as e:
            print(f"清空每周热门话题总结数据表失败: {e}")
            return False
    
    def clear_all_hot_topic_summaries(self) -> bool:
        """
        清空所有热门话题总结相关的数据表（每日和每周）
        
        Returns:
            是否成功
        """
        daily_success = self.clear_daily_hot_topics()
        weekly_success = self.clear_weekly_hot_topics()
        
        return daily_success and weekly_success
    
    def get_daily_unique_titles(self, target_date: datetime = None) -> List[Dict]:
        """
        获取当日所有快照的不重复热搜标题，按首次出现时间排序
        
        Args:
            target_date: 目标日期，默认为今天
            
        Returns:
            不重复的热搜标题列表，每个元素包含:
                - title: 标题
                - first_appear_time: 首次出现时间
                - last_appear_time: 最后出现时间
                - appear_count: 出现次数
        """
        if target_date is None:
            target_date = datetime.now()
        
        start_of_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = target_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        try:
            with duckdb.connect(self.db_path) as conn:
                results = conn.execute("""
                    SELECT 
                        title,
                        MIN(snapshot_time) as first_appear_time,
                        MAX(snapshot_time) as last_appear_time,
                        COUNT(*) as appear_count
                    FROM hot_search_items
                    WHERE snapshot_time >= ? AND snapshot_time <= ?
                    GROUP BY title
                    ORDER BY first_appear_time ASC
                """, [start_of_day, end_of_day]).fetchall()
                
                unique_titles = []
                for row in results:
                    title, first_appear, last_appear, appear_count = row
                    unique_titles.append({
                        'title': title,
                        'first_appear_time': first_appear.isoformat() if first_appear else None,
                        'last_appear_time': last_appear.isoformat() if last_appear else None,
                        'appear_count': appear_count
                    })
                
                return unique_titles
                
        except Exception as e:
            print(f"获取当日不重复热搜标题失败: {e}")
            return []
    
    def get_daily_titles_by_snapshot(self, target_date: datetime = None) -> List[Dict]:
        """
        获取当日所有快照的完整热搜标题列表（按时间顺序，带快照时间）
        
        Args:
            target_date: 目标日期，默认为今天
            
        Returns:
            按快照时间排序的标题列表，每个元素包含:
                - snapshot_time: 快照时间
                - title: 标题
        """
        if target_date is None:
            target_date = datetime.now()
        
        start_of_day = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = target_date.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        try:
            with duckdb.connect(self.db_path) as conn:
                results = conn.execute("""
                    SELECT 
                        snapshot_time,
                        title
                    FROM hot_search_items
                    WHERE snapshot_time >= ? AND snapshot_time <= ?
                    ORDER BY snapshot_time ASC, rank ASC
                """, [start_of_day, end_of_day]).fetchall()
                
                title_list = []
                for row in results:
                    snapshot_time, title = row
                    title_list.append({
                        'snapshot_time': snapshot_time.isoformat() if snapshot_time else None,
                        'title': title
                    })
                
                return title_list
                
        except Exception as e:
            print(f"获取当日快照标题列表失败: {e}")
            return []
    
    def save_investment_topic_analysis(
        self,
        analysis_date: datetime,
        analysis_result: Dict[str, Any]
    ) -> Optional[int]:
        """
        保存投资题材分析结果到数据库
        
        Args:
            analysis_date: 分析日期
            analysis_result: 分析结果字典，格式如下：
                {
                    'analysis_summary': '整体分析摘要',
                    'investment_topics': [
                        {
                            'topic_name': '题材名称',
                            'related_industries': ['行业1', '行业2'],
                            'core_logic': '核心投资逻辑',
                            'potential_beneficiary_stocks': [
                                {'stock_name': '股票名', 'stock_code': '代码', 'benefit_reason': '原因'}
                            ],
                            'market_expectation': '市场预期',
                            'analysis_dimension': '分析维度',
                            'related_hot_titles': ['相关热搜标题'],
                            'confidence_level': '高/中/低'
                        }
                    ],
                    'raw_data_summary': {
                        'total_snapshot_records': 0,
                        'total_unique_topics': 0
                    }
                }
            
        Returns:
            分析记录ID，如果保存失败返回None
        """
        try:
            analysis_date_only = analysis_date.date()
            
            with duckdb.connect(self.db_path) as conn:
                existing = conn.execute("""
                    SELECT id FROM investment_topic_analyses WHERE analysis_date = ?
                """, [analysis_date_only]).fetchone()
                
                if existing:
                    analysis_id = existing[0]
                    conn.execute("""
                        DELETE FROM topic_related_hot_titles 
                        WHERE topic_id IN (SELECT id FROM investment_topics WHERE analysis_id = ?)
                    """, [analysis_id])
                    conn.execute("""
                        DELETE FROM beneficiary_stocks 
                        WHERE topic_id IN (SELECT id FROM investment_topics WHERE analysis_id = ?)
                    """, [analysis_id])
                    conn.execute("""
                        DELETE FROM investment_topics WHERE analysis_id = ?
                    """, [analysis_id])
                    
                    conn.execute("""
                        UPDATE investment_topic_analyses 
                        SET analysis_summary = ?, total_topics = ?, 
                            total_snapshot_records = ?, total_unique_topics = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, [
                        analysis_result.get('analysis_summary', ''),
                        len(analysis_result.get('investment_topics', [])),
                        analysis_result.get('raw_data_summary', {}).get('total_snapshot_records', 0),
                        analysis_result.get('raw_data_summary', {}).get('total_unique_topics', 0),
                        analysis_id
                    ])
                else:
                    result = conn.execute("""
                        INSERT INTO investment_topic_analyses 
                        (analysis_date, analysis_summary, total_topics, 
                         total_snapshot_records, total_unique_topics, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        RETURNING id
                    """, [
                        analysis_date_only,
                        analysis_result.get('analysis_summary', ''),
                        len(analysis_result.get('investment_topics', [])),
                        analysis_result.get('raw_data_summary', {}).get('total_snapshot_records', 0),
                        analysis_result.get('raw_data_summary', {}).get('total_unique_topics', 0)
                    ]).fetchone()
                    
                    if not result:
                        print("保存投资题材分析失败")
                        return None
                    
                    analysis_id = result[0]
                
                investment_topics = analysis_result.get('investment_topics', [])
                for rank, topic in enumerate(investment_topics, 1):
                    related_industries_json = json.dumps(topic.get('related_industries', []), ensure_ascii=False) if topic.get('related_industries') else None
                    
                    topic_result = conn.execute("""
                        INSERT INTO investment_topics 
                        (analysis_id, rank, topic_name, analysis_dimension, confidence_level,
                         core_logic, market_expectation, related_industries_json, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        RETURNING id
                    """, [
                        analysis_id,
                        rank,
                        topic.get('topic_name', ''),
                        topic.get('analysis_dimension'),
                        topic.get('confidence_level'),
                        topic.get('core_logic'),
                        topic.get('market_expectation'),
                        related_industries_json
                    ]).fetchone()
                    
                    if not topic_result:
                        continue
                    
                    topic_id = topic_result[0]
                    
                    beneficiary_stocks = topic.get('potential_beneficiary_stocks', [])
                    for stock in beneficiary_stocks:
                        conn.execute("""
                            INSERT INTO beneficiary_stocks 
                            (topic_id, stock_name, stock_code, benefit_reason, created_at)
                            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                        """, [
                            topic_id,
                            stock.get('stock_name', ''),
                            stock.get('stock_code'),
                            stock.get('benefit_reason') or stock.get('benefit_reason')
                        ])
                    
                    related_hot_titles = topic.get('related_hot_titles', [])
                    for hot_title in related_hot_titles:
                        conn.execute("""
                            INSERT INTO topic_related_hot_titles 
                            (topic_id, hot_title, created_at)
                            VALUES (?, ?, CURRENT_TIMESTAMP)
                        """, [topic_id, hot_title])
                
                conn.commit()
                
                print(f"投资题材分析已保存: 日期={analysis_date_only}, 题材数={len(investment_topics)}")
                return analysis_id
                
        except Exception as e:
            print(f"保存投资题材分析失败: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def get_investment_topic_analysis(self, analysis_date: datetime) -> Optional[Dict[str, Any]]:
        """
        获取指定日期的投资题材分析结果
        
        Args:
            analysis_date: 分析日期
            
        Returns:
            分析结果字典，如果不存在返回None
        """
        try:
            analysis_date_only = analysis_date.date()
            
            with duckdb.connect(self.db_path) as conn:
                analysis = conn.execute("""
                    SELECT id, analysis_date, analysis_summary, total_topics, 
                           total_snapshot_records, total_unique_topics, created_at, updated_at
                    FROM investment_topic_analyses
                    WHERE analysis_date = ?
                """, [analysis_date_only]).fetchone()
                
                if not analysis:
                    return None
                
                analysis_id, date, analysis_summary, total_topics, \
                    total_snapshot_records, total_unique_topics, created_at, updated_at = analysis
                
                topics = conn.execute("""
                    SELECT id, rank, topic_name, analysis_dimension, confidence_level,
                           core_logic, market_expectation, related_industries_json
                    FROM investment_topics
                    WHERE analysis_id = ?
                    ORDER BY rank
                """, [analysis_id]).fetchall()
                
                topic_list = []
                for topic_row in topics:
                    topic_id, rank, topic_name, analysis_dimension, confidence_level, \
                        core_logic, market_expectation, related_industries_json = topic_row
                    
                    related_industries = []
                    if related_industries_json:
                        try:
                            related_industries = json.loads(related_industries_json)
                        except:
                            pass
                    
                    stocks = conn.execute("""
                        SELECT stock_name, stock_code, benefit_reason
                        FROM beneficiary_stocks
                        WHERE topic_id = ?
                        ORDER BY id
                    """, [topic_id]).fetchall()
                    
                    beneficiary_stocks = []
                    for stock_row in stocks:
                        stock_name, stock_code, benefit_reason = stock_row
                        beneficiary_stocks.append({
                            'stock_name': stock_name,
                            'stock_code': stock_code,
                            'benefit_reason': benefit_reason
                        })
                    
                    hot_titles = conn.execute("""
                        SELECT hot_title
                        FROM topic_related_hot_titles
                        WHERE topic_id = ?
                        ORDER BY id
                    """, [topic_id]).fetchall()
                    
                    related_hot_titles = [ht[0] for ht in hot_titles]
                    
                    topic_list.append({
                        'id': topic_id,
                        'rank': rank,
                        'topic_name': topic_name,
                        'analysis_dimension': analysis_dimension,
                        'confidence_level': confidence_level,
                        'core_logic': core_logic,
                        'market_expectation': market_expectation,
                        'related_industries': related_industries,
                        'potential_beneficiary_stocks': beneficiary_stocks,
                        'related_hot_titles': related_hot_titles
                    })
                
                return {
                    'id': analysis_id,
                    'analysis_date': date.isoformat(),
                    'analysis_summary': analysis_summary,
                    'total_topics': total_topics,
                    'total_snapshot_records': total_snapshot_records,
                    'total_unique_topics': total_unique_topics,
                    'topics': topic_list,
                    'created_at': created_at.isoformat() if created_at else None,
                    'updated_at': updated_at.isoformat() if updated_at else None
                }
                
        except Exception as e:
            print(f"获取投资题材分析失败: {e}")
            return None
    
    def get_investment_topic_analyses_by_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        include_topics: bool = True
    ) -> List[Dict[str, Any]]:
        """
        获取日期范围内的投资题材分析列表
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            include_topics: 是否包含题材详情
            
        Returns:
            分析列表
        """
        try:
            start_date_only = start_date.date()
            end_date_only = end_date.date()
            
            with duckdb.connect(self.db_path) as conn:
                analyses = conn.execute("""
                    SELECT id, analysis_date, analysis_summary, total_topics, 
                           total_snapshot_records, total_unique_topics, created_at, updated_at
                    FROM investment_topic_analyses
                    WHERE analysis_date >= ? AND analysis_date <= ?
                    ORDER BY analysis_date DESC
                """, [start_date_only, end_date_only]).fetchall()
                
                result = []
                for analysis_row in analyses:
                    analysis_id, date, analysis_summary, total_topics, \
                        total_snapshot_records, total_unique_topics, created_at, updated_at = analysis_row
                    
                    analysis_data = {
                        'id': analysis_id,
                        'analysis_date': date.isoformat(),
                        'analysis_summary': analysis_summary,
                        'total_topics': total_topics,
                        'total_snapshot_records': total_snapshot_records,
                        'total_unique_topics': total_unique_topics,
                        'created_at': created_at.isoformat() if created_at else None,
                        'updated_at': updated_at.isoformat() if updated_at else None
                    }
                    
                    if include_topics:
                        topics = conn.execute("""
                            SELECT id, rank, topic_name, analysis_dimension, confidence_level,
                                   core_logic, market_expectation, related_industries_json
                            FROM investment_topics
                            WHERE analysis_id = ?
                            ORDER BY rank
                        """, [analysis_id]).fetchall()
                        
                        topic_list = []
                        for topic_row in topics:
                            topic_id, rank, topic_name, analysis_dimension, confidence_level, \
                                core_logic, market_expectation, related_industries_json = topic_row
                            
                            related_industries = []
                            if related_industries_json:
                                try:
                                    related_industries = json.loads(related_industries_json)
                                except:
                                    pass
                            
                            stocks = conn.execute("""
                                SELECT stock_name, stock_code, benefit_reason
                                FROM beneficiary_stocks
                                WHERE topic_id = ?
                                ORDER BY id
                            """, [topic_id]).fetchall()
                            
                            beneficiary_stocks = []
                            for stock_row in stocks:
                                stock_name, stock_code, benefit_reason = stock_row
                                beneficiary_stocks.append({
                                    'stock_name': stock_name,
                                    'stock_code': stock_code,
                                    'benefit_reason': benefit_reason
                                })
                            
                            hot_titles = conn.execute("""
                                SELECT hot_title
                                FROM topic_related_hot_titles
                                WHERE topic_id = ?
                                ORDER BY id
                            """, [topic_id]).fetchall()
                            
                            related_hot_titles = [ht[0] for ht in hot_titles]
                            
                            topic_list.append({
                                'id': topic_id,
                                'rank': rank,
                                'topic_name': topic_name,
                                'analysis_dimension': analysis_dimension,
                                'confidence_level': confidence_level,
                                'core_logic': core_logic,
                                'market_expectation': market_expectation,
                                'related_industries': related_industries,
                                'potential_beneficiary_stocks': beneficiary_stocks,
                                'related_hot_titles': related_hot_titles
                            })
                        
                        analysis_data['topics'] = topic_list
                    
                    result.append(analysis_data)
                
                return result
                
        except Exception as e:
            print(f"获取投资题材分析列表失败: {e}")
            return []
    
    def clear_investment_topic_analyses(self) -> bool:
        """
        清空投资题材分析相关的数据表
        
        Returns:
            是否成功
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                conn.execute("DELETE FROM topic_related_hot_titles")
                conn.execute("DELETE FROM beneficiary_stocks")
                conn.execute("DELETE FROM investment_topics")
                conn.execute("DELETE FROM investment_topic_analyses")
                conn.commit()
                
                print("已清空投资题材分析相关数据表")
                return True
                
        except Exception as e:
            print(f"清空投资题材分析数据表失败: {e}")
            return False
    
    def get_investment_analysis_count(self) -> int:
        """
        获取投资题材分析记录总数
        
        Returns:
            记录数量
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                result = conn.execute("SELECT COUNT(*) FROM investment_topic_analyses").fetchone()
                return result[0] if result else 0
        except Exception as e:
            print(f"获取投资题材分析数量失败: {e}")
            return 0
    
    def get_alert_config(self, config_name: str = 'default') -> Optional[Dict[str, Any]]:
        """
        获取预警配置
        
        Args:
            config_name: 配置名称，默认为 'default'
            
        Returns:
            配置字典，如果不存在返回None
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                config = conn.execute("""
                    SELECT id, config_name, enabled,
                           new_topic_enabled, new_topic_top_rank_threshold,
                           rank_surge_enabled, rank_surge_threshold, rank_surge_target_rank,
                           heat_surge_enabled, heat_surge_ratio_threshold,
                           sudden_disappear_enabled, sudden_disappear_days_threshold,
                           rank_plunge_enabled, rank_plunge_threshold, rank_plunge_start_rank,
                           alert_level_normal_enabled, alert_level_important_enabled, alert_level_urgent_enabled,
                           normal_push_interval_minutes, important_push_interval_minutes, urgent_push_interval_minutes,
                           created_at, updated_at
                    FROM alert_configs
                    WHERE config_name = ?
                    LIMIT 1
                """, [config_name]).fetchone()
                
                if not config:
                    return None
                
                (id, config_name, enabled,
                 new_topic_enabled, new_topic_top_rank_threshold,
                 rank_surge_enabled, rank_surge_threshold, rank_surge_target_rank,
                 heat_surge_enabled, heat_surge_ratio_threshold,
                 sudden_disappear_enabled, sudden_disappear_days_threshold,
                 rank_plunge_enabled, rank_plunge_threshold, rank_plunge_start_rank,
                 alert_level_normal_enabled, alert_level_important_enabled, alert_level_urgent_enabled,
                 normal_push_interval_minutes, important_push_interval_minutes, urgent_push_interval_minutes,
                 created_at, updated_at) = config
                
                return {
                    'id': id,
                    'config_name': config_name,
                    'enabled': enabled,
                    'new_topic_enabled': new_topic_enabled,
                    'new_topic_top_rank_threshold': new_topic_top_rank_threshold,
                    'rank_surge_enabled': rank_surge_enabled,
                    'rank_surge_threshold': rank_surge_threshold,
                    'rank_surge_target_rank': rank_surge_target_rank,
                    'heat_surge_enabled': heat_surge_enabled,
                    'heat_surge_ratio_threshold': heat_surge_ratio_threshold,
                    'sudden_disappear_enabled': sudden_disappear_enabled,
                    'sudden_disappear_days_threshold': sudden_disappear_days_threshold,
                    'rank_plunge_enabled': rank_plunge_enabled,
                    'rank_plunge_threshold': rank_plunge_threshold,
                    'rank_plunge_start_rank': rank_plunge_start_rank,
                    'alert_level_normal_enabled': alert_level_normal_enabled,
                    'alert_level_important_enabled': alert_level_important_enabled,
                    'alert_level_urgent_enabled': alert_level_urgent_enabled,
                    'normal_push_interval_minutes': normal_push_interval_minutes,
                    'important_push_interval_minutes': important_push_interval_minutes,
                    'urgent_push_interval_minutes': urgent_push_interval_minutes,
                    'created_at': created_at.isoformat() if created_at else None,
                    'updated_at': updated_at.isoformat() if updated_at else None
                }
        except Exception as e:
            print(f"获取预警配置失败: {e}")
            return None
    
    def save_alert_config(self, config_data: Dict[str, Any], config_name: str = 'default') -> bool:
        """
        保存预警配置
        
        Args:
            config_data: 配置字典
            config_name: 配置名称
            
        Returns:
            是否保存成功
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                existing = conn.execute("""
                    SELECT id FROM alert_configs WHERE config_name = ?
                """, [config_name]).fetchone()
                
                if existing:
                    config_id = existing[0]
                    conn.execute("""
                        UPDATE alert_configs SET
                            enabled = ?,
                            new_topic_enabled = ?,
                            new_topic_top_rank_threshold = ?,
                            rank_surge_enabled = ?,
                            rank_surge_threshold = ?,
                            rank_surge_target_rank = ?,
                            heat_surge_enabled = ?,
                            heat_surge_ratio_threshold = ?,
                            sudden_disappear_enabled = ?,
                            sudden_disappear_days_threshold = ?,
                            rank_plunge_enabled = ?,
                            rank_plunge_threshold = ?,
                            rank_plunge_start_rank = ?,
                            alert_level_normal_enabled = ?,
                            alert_level_important_enabled = ?,
                            alert_level_urgent_enabled = ?,
                            normal_push_interval_minutes = ?,
                            important_push_interval_minutes = ?,
                            urgent_push_interval_minutes = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, [
                        config_data.get('enabled', True),
                        config_data.get('new_topic_enabled', True),
                        config_data.get('new_topic_top_rank_threshold', 10),
                        config_data.get('rank_surge_enabled', True),
                        config_data.get('rank_surge_threshold', 30),
                        config_data.get('rank_surge_target_rank', 10),
                        config_data.get('heat_surge_enabled', True),
                        config_data.get('heat_surge_ratio_threshold', 2.0),
                        config_data.get('sudden_disappear_enabled', True),
                        config_data.get('sudden_disappear_days_threshold', 3),
                        config_data.get('rank_plunge_enabled', True),
                        config_data.get('rank_plunge_threshold', 20),
                        config_data.get('rank_plunge_start_rank', 10),
                        config_data.get('alert_level_normal_enabled', True),
                        config_data.get('alert_level_important_enabled', True),
                        config_data.get('alert_level_urgent_enabled', True),
                        config_data.get('normal_push_interval_minutes', 60),
                        config_data.get('important_push_interval_minutes', 30),
                        config_data.get('urgent_push_interval_minutes', 5),
                        config_id
                    ])
                else:
                    conn.execute("""
                        INSERT INTO alert_configs (
                            config_name, enabled,
                            new_topic_enabled, new_topic_top_rank_threshold,
                            rank_surge_enabled, rank_surge_threshold, rank_surge_target_rank,
                            heat_surge_enabled, heat_surge_ratio_threshold,
                            sudden_disappear_enabled, sudden_disappear_days_threshold,
                            rank_plunge_enabled, rank_plunge_threshold, rank_plunge_start_rank,
                            alert_level_normal_enabled, alert_level_important_enabled, alert_level_urgent_enabled,
                            normal_push_interval_minutes, important_push_interval_minutes, urgent_push_interval_minutes
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        config_name,
                        config_data.get('enabled', True),
                        config_data.get('new_topic_enabled', True),
                        config_data.get('new_topic_top_rank_threshold', 10),
                        config_data.get('rank_surge_enabled', True),
                        config_data.get('rank_surge_threshold', 30),
                        config_data.get('rank_surge_target_rank', 10),
                        config_data.get('heat_surge_enabled', True),
                        config_data.get('heat_surge_ratio_threshold', 2.0),
                        config_data.get('sudden_disappear_enabled', True),
                        config_data.get('sudden_disappear_days_threshold', 3),
                        config_data.get('rank_plunge_enabled', True),
                        config_data.get('rank_plunge_threshold', 20),
                        config_data.get('rank_plunge_start_rank', 10),
                        config_data.get('alert_level_normal_enabled', True),
                        config_data.get('alert_level_important_enabled', True),
                        config_data.get('alert_level_urgent_enabled', True),
                        config_data.get('normal_push_interval_minutes', 60),
                        config_data.get('important_push_interval_minutes', 30),
                        config_data.get('urgent_push_interval_minutes', 5)
                    ])
                
                conn.commit()
                print(f"预警配置已保存: {config_name}")
                return True
        except Exception as e:
            print(f"保存预警配置失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def get_alert_keywords(self) -> List[Dict[str, Any]]:
        """
        获取所有关注关键词
        
        Returns:
            关键词列表
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                keywords = conn.execute("""
                    SELECT id, keyword, is_included, priority, created_at
                    FROM alert_keywords
                    ORDER BY priority DESC, created_at DESC
                """).fetchall()
                
                result = []
                for row in keywords:
                    id, keyword, is_included, priority, created_at = row
                    result.append({
                        'id': id,
                        'keyword': keyword,
                        'is_included': is_included,
                        'priority': priority,
                        'created_at': created_at.isoformat() if created_at else None
                    })
                
                return result
        except Exception as e:
            print(f"获取关注关键词失败: {e}")
            return []
    
    def add_alert_keyword(self, keyword: str, is_included: bool = True, priority: int = 0) -> Optional[int]:
        """
        添加关注关键词
        
        Args:
            keyword: 关键词
            is_included: 是否包含（True=关注，False=排除）
            priority: 优先级
            
        Returns:
            关键词ID，失败返回None
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                existing = conn.execute("""
                    SELECT id FROM alert_keywords WHERE keyword = ?
                """, [keyword]).fetchone()
                
                if existing:
                    conn.execute("""
                        UPDATE alert_keywords SET
                            is_included = ?,
                            priority = ?
                        WHERE keyword = ?
                    """, [is_included, priority, keyword])
                    conn.commit()
                    return existing[0]
                
                result = conn.execute("""
                    INSERT INTO alert_keywords (keyword, is_included, priority, created_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                    RETURNING id
                """, [keyword, is_included, priority]).fetchone()
                
                conn.commit()
                
                if result:
                    print(f"已添加关注关键词: {keyword}")
                    return result[0]
                return None
        except Exception as e:
            print(f"添加关注关键词失败: {e}")
            return None
    
    def remove_alert_keyword(self, keyword: str) -> bool:
        """
        删除关注关键词
        
        Args:
            keyword: 关键词
            
        Returns:
            是否删除成功
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                conn.execute("""
                    DELETE FROM alert_keywords WHERE keyword = ?
                """, [keyword])
                conn.commit()
                print(f"已删除关注关键词: {keyword}")
                return True
        except Exception as e:
            print(f"删除关注关键词失败: {e}")
            return False
    
    def save_alert_event(self, event_data: Dict[str, Any]) -> Optional[int]:
        """
        保存预警事件
        
        Args:
            event_data: 事件数据字典，包含:
                - alert_type: 预警类型 (new_topic, rank_surge, heat_surge, sudden_disappear, rank_plunge)
                - alert_level: 预警级别 (normal, important, urgent)
                - title: 热搜标题
                - rank_before: 之前排名
                - rank_after: 之后排名
                - rank_change: 排名变化
                - hot_value_before: 之前热度值
                - hot_value_after: 之后热度值
                - heat_change_ratio: 热度变化比例
                - snapshot_time_before: 之前快照时间
                - snapshot_time_after: 之后快照时间
                - details: 详情描述
                
        Returns:
            事件ID，失败返回None
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                result = conn.execute("""
                    INSERT INTO alert_events (
                        alert_type, alert_level, title,
                        rank_before, rank_after, rank_change,
                        hot_value_before, hot_value_after, heat_change_ratio,
                        snapshot_time_before, snapshot_time_after, details,
                        is_pushed, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FALSE, CURRENT_TIMESTAMP)
                    RETURNING id
                """, [
                    event_data.get('alert_type', ''),
                    event_data.get('alert_level', 'normal'),
                    event_data.get('title', ''),
                    event_data.get('rank_before'),
                    event_data.get('rank_after'),
                    event_data.get('rank_change'),
                    event_data.get('hot_value_before'),
                    event_data.get('hot_value_after'),
                    event_data.get('heat_change_ratio'),
                    event_data.get('snapshot_time_before'),
                    event_data.get('snapshot_time_after'),
                    event_data.get('details')
                ]).fetchone()
                
                conn.commit()
                
                if result:
                    print(f"预警事件已保存: {event_data.get('alert_type')} - {event_data.get('title')}")
                    return result[0]
                return None
        except Exception as e:
            print(f"保存预警事件失败: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def get_alert_events(
        self,
        alert_type: str = None,
        alert_level: str = None,
        start_time: datetime = None,
        end_time: datetime = None,
        title_keyword: str = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        查询预警事件
        
        Args:
            alert_type: 预警类型筛选
            alert_level: 预警级别筛选
            start_time: 开始时间
            end_time: 结束时间
            title_keyword: 标题关键词
            limit: 返回数量限制
            offset: 偏移量
            
        Returns:
            预警事件列表
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                conditions = ["1=1"]
                params = []
                
                if alert_type:
                    conditions.append("alert_type = ?")
                    params.append(alert_type)
                
                if alert_level:
                    conditions.append("alert_level = ?")
                    params.append(alert_level)
                
                if start_time:
                    conditions.append("created_at >= ?")
                    params.append(start_time)
                
                if end_time:
                    conditions.append("created_at <= ?")
                    params.append(end_time)
                
                if title_keyword:
                    conditions.append("title LIKE ?")
                    params.append(f"%{title_keyword}%")
                
                where_clause = " AND ".join(conditions)
                
                query = f"""
                    SELECT id, alert_type, alert_level, title,
                           rank_before, rank_after, rank_change,
                           hot_value_before, hot_value_after, heat_change_ratio,
                           snapshot_time_before, snapshot_time_after, details,
                           is_pushed, pushed_at, created_at
                    FROM alert_events
                    WHERE {where_clause}
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                """
                
                params.extend([limit, offset])
                
                events = conn.execute(query, params).fetchall()
                
                result = []
                for row in events:
                    (id, alert_type, alert_level, title,
                     rank_before, rank_after, rank_change,
                     hot_value_before, hot_value_after, heat_change_ratio,
                     snapshot_time_before, snapshot_time_after, details,
                     is_pushed, pushed_at, created_at) = row
                    
                    result.append({
                        'id': id,
                        'alert_type': alert_type,
                        'alert_level': alert_level,
                        'title': title,
                        'rank_before': rank_before,
                        'rank_after': rank_after,
                        'rank_change': rank_change,
                        'hot_value_before': hot_value_before,
                        'hot_value_after': hot_value_after,
                        'heat_change_ratio': heat_change_ratio,
                        'snapshot_time_before': snapshot_time_before.isoformat() if snapshot_time_before else None,
                        'snapshot_time_after': snapshot_time_after.isoformat() if snapshot_time_after else None,
                        'details': details,
                        'is_pushed': is_pushed,
                        'pushed_at': pushed_at.isoformat() if pushed_at else None,
                        'created_at': created_at.isoformat() if created_at else None
                    })
                
                return result
        except Exception as e:
            print(f"查询预警事件失败: {e}")
            return []
    
    def get_alert_events_count(
        self,
        alert_type: str = None,
        alert_level: str = None,
        start_time: datetime = None,
        end_time: datetime = None,
        title_keyword: str = None
    ) -> int:
        """
        获取预警事件数量
        
        Args:
            alert_type: 预警类型筛选
            alert_level: 预警级别筛选
            start_time: 开始时间
            end_time: 结束时间
            title_keyword: 标题关键词
            
        Returns:
            事件数量
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                conditions = ["1=1"]
                params = []
                
                if alert_type:
                    conditions.append("alert_type = ?")
                    params.append(alert_type)
                
                if alert_level:
                    conditions.append("alert_level = ?")
                    params.append(alert_level)
                
                if start_time:
                    conditions.append("created_at >= ?")
                    params.append(start_time)
                
                if end_time:
                    conditions.append("created_at <= ?")
                    params.append(end_time)
                
                if title_keyword:
                    conditions.append("title LIKE ?")
                    params.append(f"%{title_keyword}%")
                
                where_clause = " AND ".join(conditions)
                
                query = f"SELECT COUNT(*) FROM alert_events WHERE {where_clause}"
                
                result = conn.execute(query, params).fetchone()
                return result[0] if result else 0
        except Exception as e:
            print(f"获取预警事件数量失败: {e}")
            return 0
    
    def mark_alert_pushed(self, event_id: int) -> bool:
        """
        标记预警事件已推送
        
        Args:
            event_id: 事件ID
            
        Returns:
            是否成功
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                conn.execute("""
                    UPDATE alert_events 
                    SET is_pushed = TRUE, pushed_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, [event_id])
                conn.commit()
                return True
        except Exception as e:
            print(f"标记预警推送状态失败: {e}")
            return False
    
    def get_alert_statistics(self, days: int = 7) -> Dict[str, Any]:
        """
        获取预警统计数据
        
        Args:
            days: 统计天数
            
        Returns:
            统计数据字典
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                end_time = datetime.now()
                start_time = end_time - timedelta(days=days)
                
                total_count = conn.execute("""
                    SELECT COUNT(*) FROM alert_events
                    WHERE created_at >= ? AND created_at <= ?
                """, [start_time, end_time]).fetchone()[0]
                
                by_type = conn.execute("""
                    SELECT alert_type, COUNT(*) as cnt
                    FROM alert_events
                    WHERE created_at >= ? AND created_at <= ?
                    GROUP BY alert_type
                    ORDER BY cnt DESC
                """, [start_time, end_time]).fetchall()
                
                by_level = conn.execute("""
                    SELECT alert_level, COUNT(*) as cnt
                    FROM alert_events
                    WHERE created_at >= ? AND created_at <= ?
                    GROUP BY alert_level
                    ORDER BY cnt DESC
                """, [start_time, end_time]).fetchall()
                
                by_date = conn.execute("""
                    SELECT 
                        DATE(created_at) as event_date,
                        COUNT(*) as cnt
                    FROM alert_events
                    WHERE created_at >= ? AND created_at <= ?
                    GROUP BY DATE(created_at)
                    ORDER BY event_date
                """, [start_time, end_time]).fetchall()
                
                top_topics = conn.execute("""
                    SELECT title, COUNT(*) as cnt
                    FROM alert_events
                    WHERE created_at >= ? AND created_at <= ?
                    GROUP BY title
                    ORDER BY cnt DESC
                    LIMIT 10
                """, [start_time, end_time]).fetchall()
                
                return {
                    'period_days': days,
                    'total_count': total_count,
                    'by_type': {row[0]: row[1] for row in by_type},
                    'by_level': {row[0]: row[1] for row in by_level},
                    'by_date': [
                        {'date': row[0].isoformat() if row[0] else None, 'count': row[1]}
                        for row in by_date
                    ],
                    'top_topics': [
                        {'title': row[0], 'count': row[1]}
                        for row in top_topics
                    ]
                }
        except Exception as e:
            print(f"获取预警统计失败: {e}")
            return {'period_days': days, 'total_count': 0, 'error': str(e)}
    
    def get_topic_rank_history(self, title: str, hours: int = 24) -> List[Dict[str, Any]]:
        """
        获取指定话题的排名历史（用于异常检测）
        
        Args:
            title: 热搜标题
            hours: 查询小时数
            
        Returns:
            排名历史列表，按时间升序排列
        """
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(hours=hours)
            
            with duckdb.connect(self.db_path) as conn:
                results = conn.execute("""
                    SELECT snapshot_time, rank, hot_value, hot
                    FROM hot_search_items
                    WHERE title = ? AND snapshot_time >= ? AND snapshot_time <= ?
                    ORDER BY snapshot_time ASC
                """, [title, start_time, end_time]).fetchall()
                
                history = []
                for row in results:
                    snapshot_time, rank, hot_value, hot = row
                    history.append({
                        'snapshot_time': snapshot_time.isoformat() if snapshot_time else None,
                        'rank': rank,
                        'hot_value': hot_value,
                        'hot': hot
                    })
                
                return history
        except Exception as e:
            print(f"获取话题排名历史失败: {e}")
            return []
    
    def get_consecutive_appear_days(self, title: str) -> int:
        """
        计算话题连续上榜天数
        
        Args:
            title: 热搜标题
            
        Returns:
            连续上榜天数
        """
        try:
            with duckdb.connect(self.db_path) as conn:
                results = conn.execute("""
                    SELECT DISTINCT DATE(snapshot_time) as appear_date
                    FROM hot_search_items
                    WHERE title = ?
                    ORDER BY appear_date DESC
                """, [title]).fetchall()
                
                if not results:
                    return 0
                
                dates = [row[0] for row in results]
                consecutive_days = 0
                expected_date = dates[0]
                
                for date in dates:
                    if date == expected_date:
                        consecutive_days += 1
                        expected_date = date - timedelta(days=1)
                    else:
                        break
                
                return consecutive_days
        except Exception as e:
            print(f"计算连续上榜天数失败: {e}")
            return 0


def main():
    """测试代码"""
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    storage = DuckDBStorage(config)
    
    print("\n=== 数据库统计 ===")
    print(f"快照总数: {storage.get_snapshot_count()}")
    print(f"热搜项总数: {storage.get_item_count()}")
    
    test_data = {
        'timestamp': datetime.now().isoformat(),
        'total_count': 5,
        'hot_list': [
            {'rank': 1, 'title': '人工智能技术突破', 'hot': '500万', 'url': 'https://test.com/1', 'is_market': False},
            {'rank': 2, 'title': '新能源汽车销量创新高', 'hot': '400万', 'url': 'https://test.com/2', 'is_market': False},
            {'rank': 3, 'title': '央行降准', 'hot': '350万', 'url': 'https://test.com/3', 'is_market': False},
            {'rank': 4, 'title': '芯片短缺问题缓解', 'hot': '300万', 'url': 'https://test.com/4', 'is_market': False},
            {'rank': 5, 'title': '消费升级趋势明显', 'hot': '250万', 'url': 'https://test.com/5', 'is_market': False}
        ]
    }
    
    print("\n=== 测试保存数据 ===")
    snapshot_id = storage.save_hot_search(test_data)
    print(f"保存的快照ID: {snapshot_id}")
    
    print("\n=== 测试获取最新快照 ===")
    latest = storage.get_latest_snapshot()
    if latest:
        print(f"最新快照时间: {latest['timestamp']}")
        print(f"热搜数量: {len(latest['hot_list'])}")
        for item in latest['hot_list'][:3]:
            print(f"  排名 {item['rank']}: {item['title']} (热度: {item['hot']})")
    
    print("\n=== 测试按时间范围查询 ===")
    end_time = datetime.now()
    start_time = end_time - timedelta(days=1)
    history = storage.get_history_by_time_range(start_time, end_time)
    print(f"找到 {len(history)} 条历史记录")
    
    print("\n=== 测试标题搜索 ===")
    results = storage.search_by_title('人工智能', limit=10)
    print(f"找到 {len(results)} 条匹配记录")
    for r in results[:3]:
        print(f"  {r['snapshot_time']} - 排名{r['rank']}: {r['title']}")
    
    print("\n=== 测试完成 ===")


if __name__ == '__main__':
    main()
