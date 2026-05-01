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
