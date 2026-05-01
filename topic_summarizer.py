#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
热门话题总结模块 - 基于快照数据识别每日和每周热门话题
"""

import json
import os
import sys
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple
import requests

from duckdb_storage import DuckDBStorage


class TopicSummarizer:
    """热门话题总结器"""
    
    def __init__(self, config: Dict):
        """
        初始化热门话题总结器
        
        Args:
            config: 配置字典
        """
        self.config = config
        self.openrouter_config = config.get('openrouter', {})
        self.api_key = self.openrouter_config.get('api_key', '')
        self.api_url = self.openrouter_config.get('api_url', 'https://openrouter.ai/api/v1/chat/completions')
        self.model = self.openrouter_config.get('model', 'gpt-4o-mini')
        
        self.storage = DuckDBStorage(config)
        
        self.summary_config = config.get('summary', {})
        self.max_snapshots_for_daily = self.summary_config.get('max_snapshots_for_daily', 5)
        self.max_topics_for_daily = self.summary_config.get('max_topics_for_daily', 20)
        self.max_topics_for_weekly = self.summary_config.get('max_topics_for_weekly', 15)
    
    def _call_ai_api(self, prompt: str, system_prompt: str = None) -> Optional[str]:
        """调用AI接口"""
        if not self.api_key:
            print("未配置OpenRouter API密钥")
            return None
        
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}',
            'HTTP-Referer': 'https://github.com/WeiboHotTradeSignal',
            'X-Title': 'Weibo-Hot-Trade-Signal-Topic-Summarizer'
        }
        
        messages = []
        
        if system_prompt:
            messages.append({
                'role': 'system',
                'content': system_prompt
            })
        
        messages.append({
            'role': 'user',
            'content': prompt
        })
        
        data = {
            'model': self.model,
            'messages': messages,
            'temperature': 0.7,
            'max_tokens': 4000
        }
        
        try:
            response = requests.post(
                self.api_url,
                headers=headers,
                json=data,
                timeout=120
            )
            response.raise_for_status()
            
            result = response.json()
            
            if 'choices' in result and len(result['choices']) > 0:
                return result['choices'][0]['message']['content']
            else:
                print(f"AI接口返回格式异常: {result}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"调用AI接口失败: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"解析AI接口响应失败: {e}")
            return None
    
    def summarize_daily_topics(self, target_date: date = None) -> Tuple[bool, List[Dict]]:
        """
        总结指定日期的热门话题
        
        Args:
            target_date: 目标日期，默认为当天
            
        Returns:
            (是否成功, 热门话题列表)
        """
        if target_date is None:
            target_date = date.today()
        
        print(f"=" * 60)
        print(f"开始总结 {target_date} 的热门话题")
        print(f"=" * 60)
        
        snapshots = self.storage.get_representative_snapshots_by_date(
            target_date, 
            self.max_snapshots_for_daily
        )
        
        if not snapshots:
            print(f"日期 {target_date} 没有快照数据，无法进行总结")
            return (False, [])
        
        print(f"获取到 {len(snapshots)} 个代表性快照")
        
        topic_stats = self.storage.get_topic_appearances_by_date(target_date)
        
        if not topic_stats:
            print(f"日期 {target_date} 没有话题统计数据")
            return (False, [])
        
        print(f"共统计到 {len(topic_stats)} 个不同话题")
        
        candidate_topics = self._select_candidate_topics(topic_stats)
        
        if not candidate_topics:
            print("没有候选话题可供分析")
            return (False, [])
        
        print(f"选择了 {len(candidate_topics)} 个候选话题进行AI分析")
        
        hot_topics = self._analyze_daily_topics_with_ai(
            snapshots, 
            candidate_topics, 
            target_date
        )
        
        if not hot_topics:
            print("AI分析未能识别出热门话题")
            return (False, [])
        
        save_success = self.storage.save_daily_hot_topics(target_date, hot_topics)
        
        if save_success:
            print(f"成功保存 {len(hot_topics)} 个每日热门话题")
        else:
            print("保存每日热门话题失败")
        
        return (save_success, hot_topics)
    
    def _select_candidate_topics(self, topic_stats: Dict[str, Dict]) -> List[Dict]:
        """
        从话题统计中选择候选话题
        
        选择标准：
        1. 出现次数较多（多次出现在热搜中）
        2. 排名靠前
        3. 热度值较高
        
        Args:
            topic_stats: 话题统计字典
            
        Returns:
            候选话题列表
        """
        topics_list = list(topic_stats.values())
        
        topics_list.sort(key=lambda x: (
            -x.get('appearance_count', 0),
            x.get('best_rank', 999),
            -x.get('avg_hot_value', 0)
        ))
        
        max_candidates = self.max_topics_for_daily * 2
        return topics_list[:max_candidates]
    
    def _analyze_daily_topics_with_ai(
        self, 
        snapshots: List[Dict], 
        candidate_topics: List[Dict], 
        target_date: date
    ) -> List[Dict]:
        """
        使用AI分析每日热门话题
        
        Args:
            snapshots: 代表性快照列表
            candidate_topics: 候选话题列表
            target_date: 目标日期
            
        Returns:
            AI识别的热门话题列表
        """
        print("开始使用AI分析每日热门话题...")
        
        system_prompt = """你是一位专业的社交媒体数据分析师，擅长从大量热搜数据中识别真正具有持久性和影响力的热门话题。

请根据提供的代表性快照数据和候选话题统计，分析当日真正热门的话题。

热门话题判定标准：
1. 持久性：在当日多次出现于热搜榜单
2. 影响力：排名靠前（如Top 10内）或热度值高
3. 重要性：话题本身具有新闻价值或社会影响力

请以JSON格式返回分析结果，格式如下：
{
  "hot_topics": [
    {
      "title": "话题标题",
      "topic_rank": 1,
      "analysis_summary": "分析摘要：说明为什么这个话题是当日热门，它的出现频率、排名变化、热度趋势等"
    }
  ]
}

注意：
- 请只返回真正具有持久性的热门话题，不要包含昙花一现的话题
- 话题排名按重要性从高到低排列
- analysis_summary应该简洁明了，突出话题的持久性和影响力
"""
        
        snapshots_summary = []
        for idx, snapshot in enumerate(snapshots):
            snapshot_time = snapshot.get('snapshot_time', '')
            hot_list = snapshot.get('hot_list', [])[:30]
            
            hot_summary = []
            for item in hot_list:
                hot_summary.append(f"排名{item['rank']}: {item['title']}")
            
            snapshots_summary.append({
                'snapshot_index': idx + 1,
                'snapshot_time': snapshot_time,
                'hot_top': hot_summary
            })
        
        candidates_summary = []
        for idx, topic in enumerate(candidate_topics):
            candidates_summary.append({
                'title': topic.get('title', ''),
                'appearance_count': topic.get('appearance_count', 0),
                'best_rank': topic.get('best_rank'),
                'avg_hot_value': topic.get('avg_hot_value'),
                'first_seen': topic.get('first_seen_time').isoformat() if topic.get('first_seen_time') else None,
                'last_seen': topic.get('last_seen_time').isoformat() if topic.get('last_seen_time') else None
            })
        
        prompt = f"""请分析以下 {target_date} 的微博热搜数据，识别当日真正具有持久性的热门话题。

【代表性快照数据】（共 {len(snapshots)} 个快照，按时间顺序排列）
{json.dumps(snapshots_summary, ensure_ascii=False, indent=2)}

【候选话题统计】（共 {len(candidates_summary)} 个候选话题，按出现次数和排名排序）
{json.dumps(candidates_summary, ensure_ascii=False, indent=2)}

请完成以下分析：
1. 从候选话题中筛选出真正具有持久性的热门话题（出现次数多、排名靠前）
2. 为每个入选的热门话题撰写分析摘要，说明：
   - 该话题出现的频率如何？
   - 该话题的排名变化趋势如何？
   - 该话题为什么值得关注？
3. 按重要性对话题进行排名

请以严格的JSON格式返回结果，不要包含其他文字说明。"""
        
        result = self._call_ai_api(prompt, system_prompt)
        
        if not result:
            print("AI接口未返回有效结果")
            return []
        
        try:
            json_start = result.find('{')
            json_end = result.rfind('}')
            if json_start != -1 and json_end != -1:
                json_str = result[json_start:json_end+1]
                parsed = json.loads(json_str)
                
                if isinstance(parsed, dict) and 'hot_topics' in parsed:
                    hot_topics = parsed['hot_topics']
                    
                    for i, topic in enumerate(hot_topics):
                        if 'topic_rank' not in topic:
                            topic['topic_rank'] = i + 1
                        
                        title = topic.get('title', '')
                        for stat_topic in candidate_topics:
                            if stat_topic.get('title') == title:
                                topic['first_seen_time'] = stat_topic.get('first_seen_time')
                                topic['last_seen_time'] = stat_topic.get('last_seen_time')
                                topic['appearance_count'] = stat_topic.get('appearance_count', 0)
                                topic['best_rank'] = stat_topic.get('best_rank')
                                topic['avg_hot_value'] = stat_topic.get('avg_hot_value')
                                topic['hot_value_max'] = stat_topic.get('hot_value_max')
                                topic['hot_value_min'] = stat_topic.get('hot_value_min')
                                break
                    
                    hot_topics = hot_topics[:self.max_topics_for_daily]
                    
                    print(f"AI识别出 {len(hot_topics)} 个每日热门话题")
                    return hot_topics
                else:
                    print("AI返回的JSON格式不符合预期")
                    return []
            else:
                print("无法从AI响应中提取JSON")
                print(f"AI响应: {result[:500] if len(result) > 500 else result}")
                return []
                
        except json.JSONDecodeError as e:
            print(f"解析AI返回的每日话题结果失败: {e}")
            print(f"AI响应: {result[:500] if len(result) > 500 else result}")
            return []
    
    def summarize_weekly_topics(self, week_start: date = None, week_end: date = None) -> Tuple[bool, List[Dict]]:
        """
        总结指定周的热门话题
        
        Args:
            week_start: 周开始日期，默认为本周一
            week_end: 周结束日期，默认为本周日
            
        Returns:
            (是否成功, 热门话题列表)
        """
        if week_start is None or week_end is None:
            today = date.today()
            week_start = today - timedelta(days=today.weekday())
            week_end = week_start + timedelta(days=6)
        
        print(f"=" * 60)
        print(f"开始总结 {week_start} ~ {week_end} 的每周热门话题")
        print(f"=" * 60)
        
        daily_topics_by_date = self.storage.get_daily_hot_topics_by_week(week_start, week_end)
        
        if not daily_topics_by_date:
            print(f"周 {week_start} ~ {week_end} 没有每日热门话题数据，无法进行每周总结")
            return (False, [])
        
        print(f"获取到 {len(daily_topics_by_date)} 天的每日热门话题数据")
        
        topic_weekly_stats = self._aggregate_weekly_topic_stats(daily_topics_by_date)
        
        if not topic_weekly_stats:
            print("没有足够的周度话题统计数据")
            return (False, [])
        
        print(f"共统计到 {len(topic_weekly_stats)} 个跨天话题")
        
        weekly_hot_topics = self._analyze_weekly_topics_with_ai(
            daily_topics_by_date, 
            topic_weekly_stats,
            week_start,
            week_end
        )
        
        if not weekly_hot_topics:
            print("AI分析未能识别出每周热门话题")
            return (False, [])
        
        save_success = self.storage.save_weekly_hot_topics(week_start, week_end, weekly_hot_topics)
        
        if save_success:
            print(f"成功保存 {len(weekly_hot_topics)} 个每周热门话题")
        else:
            print("保存每周热门话题失败")
        
        return (save_success, weekly_hot_topics)
    
    def _aggregate_weekly_topic_stats(self, daily_topics_by_date: Dict[date, List[Dict]]) -> Dict[str, Dict]:
        """
        聚合周度话题统计
        
        Args:
            daily_topics_by_date: 按日期分组的每日热门话题
            
        Returns:
            按标题聚合的周度统计字典
        """
        weekly_stats = {}
        
        for topic_date, daily_topics in daily_topics_by_date.items():
            for topic in daily_topics:
                title = topic.get('title', '')
                if not title:
                    continue
                
                if title not in weekly_stats:
                    weekly_stats[title] = {
                        'title': title,
                        'appearance_dates': [],
                        'daily_ranks': [],
                        'best_rank': None,
                        'total_appearance_count': 0,
                        'daily_analysis': []
                    }
                
                weekly_stats[title]['appearance_dates'].append(topic_date)
                
                rank = topic.get('topic_rank', 999)
                weekly_stats[title]['daily_ranks'].append({
                    'date': topic_date,
                    'rank': rank
                })
                
                if weekly_stats[title]['best_rank'] is None or rank < weekly_stats[title]['best_rank']:
                    weekly_stats[title]['best_rank'] = rank
                
                weekly_stats[title]['total_appearance_count'] += topic.get('appearance_count', 0)
                
                weekly_stats[title]['daily_analysis'].append({
                    'date': topic_date,
                    'rank': rank,
                    'analysis_summary': topic.get('analysis_summary', '')
                })
        
        for title in weekly_stats:
            weekly_stats[title]['appearance_days'] = len(weekly_stats[title]['appearance_dates'])
        
        return weekly_stats
    
    def _analyze_weekly_topics_with_ai(
        self, 
        daily_topics_by_date: Dict[date, List[Dict]], 
        topic_weekly_stats: Dict[str, Dict],
        week_start: date,
        week_end: date
    ) -> List[Dict]:
        """
        使用AI分析每周热门话题
        
        Args:
            daily_topics_by_date: 按日期分组的每日热门话题
            topic_weekly_stats: 周度话题统计
            week_start: 周开始日期
            week_end: 周结束日期
            
        Returns:
            AI识别的每周热门话题列表
        """
        print("开始使用AI分析每周热门话题...")
        
        system_prompt = """你是一位专业的社交媒体数据分析师，擅长跨时间维度分析热门话题的演变趋势。

请根据提供的每日热门话题数据，进行跨天综合分析，识别一周内真正持续受到关注的话题。

每周热门话题判定标准：
1. 持续性：在一周内多天出现在热门话题中
2. 热度演变：分析话题热度的上升/下降/稳定趋势
3. 影响力：话题在其出现的日期中排名如何

请以JSON格式返回分析结果，格式如下：
{
  "weekly_hot_topics": [
    {
      "title": "话题标题",
      "topic_rank": 1,
      "appearance_days": 5,
      "best_rank": 2,
      "trend_summary": "热度趋势摘要：如'周一首次进入Top 10，周二达到排名第2，随后几天保持在Top 5范围内'",
      "analysis_summary": "综合分析：说明为什么这个话题是本周持续热门，它的关注度变化、社会影响等"
    }
  ]
}

注意：
- 请只返回真正具有持续性的每周热门话题（至少在2天以上出现）
- 话题排名按持续性和影响力综合排序
- trend_summary要清晰描述话题在一周内的热度演变趋势
- analysis_summary要突出话题的持续性和社会意义
"""
        
        daily_summary = []
        for topic_date in sorted(daily_topics_by_date.keys()):
            daily_topics = daily_topics_by_date[topic_date]
            
            day_summary = {
                'date': str(topic_date),
                'top_topics': []
            }
            
            for topic in daily_topics[:10]:
                day_summary['top_topics'].append({
                    'rank': topic.get('topic_rank'),
                    'title': topic.get('title'),
                    'analysis': topic.get('analysis_summary', '')[:100]
                })
            
            daily_summary.append(day_summary)
        
        candidates_summary = []
        weekly_stats_list = list(topic_weekly_stats.values())
        
        weekly_stats_list.sort(key=lambda x: (
            -x.get('appearance_days', 0),
            x.get('best_rank', 999),
            -x.get('total_appearance_count', 0)
        ))
        
        for idx, stat in enumerate(weekly_stats_list[:self.max_topics_for_weekly * 2]):
            daily_ranks = stat.get('daily_ranks', [])
            daily_ranks.sort(key=lambda x: x['date'])
            
            rank_history = []
            for dr in daily_ranks:
                rank_history.append(f"{dr['date']}: 排名{dr['rank']}")
            
            candidates_summary.append({
                'title': stat.get('title', ''),
                'appearance_days': stat.get('appearance_days', 0),
                'best_rank': stat.get('best_rank'),
                'total_appearance_count': stat.get('total_appearance_count', 0),
                'rank_history': ' -> '.join(rank_history)
            })
        
        prompt = f"""请分析以下一周（{week_start} ~ {week_end}）的热门话题数据，进行跨天综合分析，识别本周持续受到关注的热门话题。

【每日热门话题摘要】
{json.dumps(daily_summary, ensure_ascii=False, indent=2)}

【候选跨天话题统计】（共 {len(candidates_summary)} 个候选，按出现天数排序）
{json.dumps(candidates_summary, ensure_ascii=False, indent=2)}

请完成以下分析：
1. 从候选话题中筛选出真正具有持续性的每周热门话题（至少2天以上出现）
2. 分析每个入选话题的热度演变趋势（上升/下降/稳定）
3. 为每个话题撰写趋势摘要和综合分析
4. 按持续性和影响力综合排名

请以严格的JSON格式返回结果，不要包含其他文字说明。"""
        
        result = self._call_ai_api(prompt, system_prompt)
        
        if not result:
            print("AI接口未返回有效结果")
            return []
        
        try:
            json_start = result.find('{')
            json_end = result.rfind('}')
            if json_start != -1 and json_end != -1:
                json_str = result[json_start:json_end+1]
                parsed = json.loads(json_str)
                
                if isinstance(parsed, dict) and 'weekly_hot_topics' in parsed:
                    weekly_topics = parsed['weekly_hot_topics']
                    
                    for i, topic in enumerate(weekly_topics):
                        if 'topic_rank' not in topic:
                            topic['topic_rank'] = i + 1
                        
                        title = topic.get('title', '')
                        for stat in weekly_stats_list:
                            if stat.get('title') == title:
                                if 'appearance_days' not in topic:
                                    topic['appearance_days'] = stat.get('appearance_days', 0)
                                if 'best_rank' not in topic:
                                    topic['best_rank'] = stat.get('best_rank')
                                break
                    
                    weekly_topics = weekly_topics[:self.max_topics_for_weekly]
                    
                    print(f"AI识别出 {len(weekly_topics)} 个每周热门话题")
                    return weekly_topics
                else:
                    print("AI返回的JSON格式不符合预期")
                    return []
            else:
                print("无法从AI响应中提取JSON")
                print(f"AI响应: {result[:500] if len(result) > 500 else result}")
                return []
                
        except json.JSONDecodeError as e:
            print(f"解析AI返回的每周话题结果失败: {e}")
            print(f"AI响应: {result[:500] if len(result) > 500 else result}")
            return []


def load_config(config_path: str = 'config.json') -> Optional[Dict]:
    """加载配置文件"""
    if not os.path.exists(config_path):
        print(f"配置文件不存在: {config_path}")
        return None
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except json.JSONDecodeError as e:
        print(f"配置文件格式错误: {e}")
        return None
    except Exception as e:
        print(f"加载配置文件失败: {e}")
        return None


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='热门话题总结工具')
    parser.add_argument('-c', '--config', default='config.json', help='配置文件路径')
    parser.add_argument('--type', required=True, choices=['daily', 'weekly'], help='总结类型：daily(每日) 或 weekly(每周)')
    parser.add_argument('--date', help='指定日期（每日总结时使用），格式：YYYY-MM-DD')
    parser.add_argument('--week-start', help='周开始日期（每周总结时使用），格式：YYYY-MM-DD')
    parser.add_argument('--week-end', help='周结束日期（每周总结时使用），格式：YYYY-MM-DD')
    
    args = parser.parse_args()
    
    config = load_config(args.config)
    if not config:
        print("无法加载配置文件，程序退出")
        sys.exit(1)
    
    summarizer = TopicSummarizer(config)
    
    if args.type == 'daily':
        target_date = None
        if args.date:
            try:
                target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
            except ValueError:
                print(f"日期格式错误: {args.date}，请使用 YYYY-MM-DD 格式")
                sys.exit(1)
        
        success, topics = summarizer.summarize_daily_topics(target_date)
        
        if success and topics:
            print(f"\n每日热门话题总结完成！")
            print(f"共识别出 {len(topics)} 个热门话题：")
            for topic in topics:
                print(f"  {topic['topic_rank']}. {topic['title']}")
                if topic.get('analysis_summary'):
                    print(f"     分析: {topic['analysis_summary'][:80]}...")
        else:
            print("\n每日热门话题总结失败或没有识别出话题")
    
    else:
        week_start = None
        week_end = None
        
        if args.week_start:
            try:
                week_start = datetime.strptime(args.week_start, '%Y-%m-%d').date()
            except ValueError:
                print(f"周开始日期格式错误: {args.week_start}，请使用 YYYY-MM-DD 格式")
                sys.exit(1)
        
        if args.week_end:
            try:
                week_end = datetime.strptime(args.week_end, '%Y-%m-%d').date()
            except ValueError:
                print(f"周结束日期格式错误: {args.week_end}，请使用 YYYY-MM-DD 格式")
                sys.exit(1)
        
        success, topics = summarizer.summarize_weekly_topics(week_start, week_end)
        
        if success and topics:
            print(f"\n每周热门话题总结完成！")
            print(f"共识别出 {len(topics)} 个热门话题：")
            for topic in topics:
                print(f"  {topic['topic_rank']}. {topic['title']} (出现{topic.get('appearance_days', 0)}天, 最佳排名{topic.get('best_rank')})")
                if topic.get('trend_summary'):
                    print(f"     趋势: {topic['trend_summary']}")
        else:
            print("\n每周热门话题总结失败或没有识别出话题")
