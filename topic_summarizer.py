#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
热门话题总结模块 - 实现每日和每周热门话题的AI分析和总结
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple

from duckdb_storage import DuckDBStorage
from ai_analyzer import AIAnalyzer
from logger import get_logger, log_step, log_error


logger = get_logger()


class TopicSummarizer:
    """热门话题总结器"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化热门话题总结器
        
        Args:
            config: 配置字典
        """
        self.config = config
        self.storage = DuckDBStorage(config)
        self.ai_analyzer = AIAnalyzer(config)
    
    def _call_ai_with_system_prompt(self, prompt: str, system_prompt: str) -> Optional[str]:
        """
        调用AI接口，使用系统提示词
        
        Args:
            prompt: 用户提示词
            system_prompt: 系统提示词
            
        Returns:
            AI响应内容
        """
        return self.ai_analyzer._call_ai_api(prompt, system_prompt)
    
    def analyze_daily_hot_topics(self, target_date: datetime = None) -> Optional[Dict[str, Any]]:
        """
        分析每日热门话题，识别具有持久性的话题
        
        Args:
            target_date: 目标日期，默认为昨天（因为要总结当日数据，通常在晚上执行）
            
        Returns:
            分析结果字典
        """
        if target_date is None:
            target_date = datetime.now() - timedelta(days=1)
        
        log_step("每日总结", f"开始分析 {target_date.strftime('%Y-%m-%d')} 的热门话题...")
        
        snapshots = self.storage.get_daily_snapshots_for_summary(target_date)
        
        if not snapshots:
            logger.warning(f"没有找到 {target_date.strftime('%Y-%m-%d')} 的快照数据，无法进行每日总结")
            return None
        
        logger.info(f"获取到 {len(snapshots)} 个代表性快照用于分析")
        
        topic_stats = self.storage.get_topic_appearances_by_date(target_date)
        
        if not topic_stats:
            logger.warning(f"没有找到 {target_date.strftime('%Y-%m-%d')} 的话题统计数据")
            return None
        
        logger.info(f"共统计到 {len(topic_stats)} 个话题")
        
        representative_snapshots_info = []
        for snapshot in snapshots:
            snapshot_time = snapshot['snapshot_time']
            hot_list = snapshot['hot_list'][:15]
            
            hot_items = []
            for item in hot_list:
                hot_items.append(f"排名{item['rank']}: {item['title']} (热度: {item.get('hot', 'N/A')})")
            
            representative_snapshots_info.append({
                'snapshot_time': snapshot_time,
                'top_hot_topics': hot_items
            })
        
        candidate_topics = []
        for title, stats in topic_stats.items():
            appear_count = stats['appear_count']
            best_rank = stats['best_rank']
            
            is_persistent_candidate = False
            reason = ""
            
            if appear_count >= 5:
                is_persistent_candidate = True
                reason = f"当日出现 {appear_count} 次，超过5次阈值"
            elif best_rank <= 5 and appear_count >= 2:
                is_persistent_candidate = True
                reason = f"最高排名第 {best_rank} 名，且出现 {appear_count} 次"
            elif best_rank <= 3:
                is_persistent_candidate = True
                reason = f"最高排名第 {best_rank} 名，进入前3"
            
            if is_persistent_candidate:
                candidate_topics.append({
                    'title': title,
                    'appear_count': appear_count,
                    'best_rank': best_rank,
                    'avg_hot_value': stats.get('avg_hot_value'),
                    'max_hot_value': stats.get('max_hot_value'),
                    'first_appear_time': stats.get('first_appear_time'),
                    'last_appear_time': stats.get('last_appear_time'),
                    'persistence_reason': reason
                })
        
        candidate_topics.sort(key=lambda x: (-x['appear_count'], x['best_rank']))
        
        if not candidate_topics:
            logger.warning("没有找到符合条件的持久性热门话题")
            return None
        
        logger.info(f"筛选出 {len(candidate_topics)} 个候选持久性话题")
        
        ai_result = self._analyze_with_ai(
            target_date=target_date,
            snapshots_info=representative_snapshots_info,
            candidate_topics=candidate_topics,
            analysis_type='daily'
        )
        
        if ai_result:
            final_topics = self._merge_ai_result_with_stats(ai_result, candidate_topics)
            summary_text = ai_result.get('summary_text', '')
            
            return {
                'summary_date': target_date,
                'topics': final_topics,
                'summary_text': summary_text,
                'total_snapshots': len(snapshots),
                'total_topics': len(final_topics)
            }
        else:
            final_topics = []
            for idx, topic in enumerate(candidate_topics[:20]):
                final_topics.append({
                    'rank': idx + 1,
                    'title': topic['title'],
                    'appear_count': topic['appear_count'],
                    'best_rank': topic['best_rank'],
                    'avg_hot_value': topic.get('avg_hot_value'),
                    'max_hot_value': topic.get('max_hot_value'),
                    'first_appear_time': topic.get('first_appear_time'),
                    'last_appear_time': topic.get('last_appear_time'),
                    'is_persistent': True,
                    'persistence_reason': topic.get('persistence_reason', 'AI分析失败，使用默认规则')
                })
            
            return {
                'summary_date': target_date,
                'topics': final_topics,
                'summary_text': 'AI分析失败，使用默认规则筛选的热门话题',
                'total_snapshots': len(snapshots),
                'total_topics': len(final_topics)
            }
    
    def _analyze_with_ai(
        self,
        target_date: datetime,
        snapshots_info: List[Dict],
        candidate_topics: List[Dict],
        analysis_type: str = 'daily'
    ) -> Optional[Dict[str, Any]]:
        """
        使用AI分析热门话题
        
        Args:
            target_date: 目标日期
            snapshots_info: 快照信息
            candidate_topics: 候选话题列表
            analysis_type: 分析类型 'daily' 或 'weekly'
            
        Returns:
            AI分析结果
        """
        log_step("AI分析", "开始调用AI分析热门话题...")
        
        if analysis_type == 'daily':
            system_prompt = """你是一位专业的社交媒体趋势分析专家。
请根据提供的微博热搜快照数据和候选热门话题，分析当日具有持久性的热门话题。

持久性热门话题的标准：
1. 在当日多次出现（出现次数多）
2. 排名靠前（进入TOP 10，特别是TOP 5）
3. 热度值持续较高

请以JSON格式返回分析结果，格式如下：
{
  "summary_text": "对当日热门话题的整体总结描述",
  "hot_topics": [
    {
      "rank": 1,
      "title": "话题标题",
      "is_persistent": true,
      "persistence_reason": "为什么这个话题具有持久性（如：当日出现X次，最高排名第Y名，持续受到关注）",
      "heat_analysis": "对该话题热度的简要分析"
    }
  ]
}

注意：
1. 请按话题的持久性和重要性排序
2. 最多返回20个话题
3. 确保JSON格式严格正确"""
            
            snapshots_desc = ""
            for idx, snapshot in enumerate(snapshots_info):
                snapshots_desc += f"\n【快照 {idx+1} - {snapshot['snapshot_time']}】\n"
                for item in snapshot['top_hot_topics']:
                    snapshots_desc += f"  - {item}\n"
            
            candidates_desc = "\n【候选热门话题】\n"
            for idx, topic in enumerate(candidate_topics[:30]):
                candidates_desc += f"{idx+1}. {topic['title']}\n"
                candidates_desc += f"   - 出现次数: {topic['appear_count']}次\n"
                candidates_desc += f"   - 最高排名: 第{topic['best_rank']}名\n"
                if topic.get('max_hot_value'):
                    candidates_desc += f"   - 最高热度: {topic['max_hot_value']}\n"
                candidates_desc += f"   - 初步判定原因: {topic.get('persistence_reason', 'N/A')}\n\n"
            
            prompt = f"""请分析以下微博热搜数据，识别当日（{target_date.strftime('%Y-%m-%d')}）具有持久性的热门话题。

【代表性快照数据】
（为节省token，只提供当日最早、最晚和中间几个代表性快照的TOP热搜）
{snapshots_desc}

{candidates_desc}

请完成以下任务：
1. 分析这些话题的持久性，判断哪些是真正具有持续影响力的热门话题
2. 按重要性和持久性排序，给出排名
3. 为每个话题提供持久性原因分析
4. 给出整体总结

请以严格的JSON格式返回结果。"""
        
        elif analysis_type == 'weekly':
            system_prompt = """你是一位专业的社交媒体趋势分析专家。
请根据本周内的每日热门话题总结，进行跨天综合分析，识别一周内持续受到关注的话题，并分析其热度演变趋势。

持续性热门话题的标准：
1. 连续多天出现在每日热门话题中
2. 热度持续稳定或呈上升趋势
3. 排名保持在较高位置

请以JSON格式返回分析结果，格式如下：
{
  "summary_text": "对本周热门话题的整体总结描述，包括主要趋势和热点演变",
  "hot_topics": [
    {
      "rank": 1,
      "title": "话题标题",
      "appear_days": 5,
      "is_sustained": true,
      "sustained_reason": "为什么这个话题具有持续性（如：连续X天出现在热门榜，热度呈上升趋势）",
      "heat_trend": "热度趋势描述（上升/稳定/下降）",
      "heat_evolution": "详细的热度演变分析，包括每天的排名变化和热度变化"
    }
  ]
}

注意：
1. 请按话题的持续性和重要性排序
2. 最多返回20个话题
3. 确保JSON格式严格正确"""
            
            daily_summaries_desc = "\n【本周每日热门话题总结】\n"
            for idx, daily_summary in enumerate(snapshots_info):
                daily_summaries_desc += f"\n===== {daily_summary['summary_date']} =====\n"
                for topic in daily_summary['topics'][:10]:
                    daily_summaries_desc += f"- 排名{topic['rank']}: {topic['title']}\n"
                    daily_summaries_desc += f"  出现次数: {topic['appear_count']}次, 最高排名: 第{topic['best_rank']}名\n"
            
            prompt = f"""请分析以下一周内的每日热门话题数据，进行跨天综合分析。

分析周期：{target_date[0].strftime('%Y-%m-%d')} 至 {target_date[1].strftime('%Y-%m-%d')}

{daily_summaries_desc}

请完成以下任务：
1. 识别一周内持续受到关注的话题（跨天出现的话题）
2. 分析每个持续性话题的热度演变趋势（上升/稳定/下降）
3. 按持续性和重要性排序，给出排名
4. 为每个话题提供持续性原因分析和热度演变详情
5. 给出整体总结

请以严格的JSON格式返回结果。"""
        
        else:
            logger.error(f"不支持的分析类型: {analysis_type}")
            return None
        
        result = self._call_ai_with_system_prompt(prompt, system_prompt)
        
        if not result:
            logger.warning("AI接口未返回有效结果")
            return None
        
        try:
            json_start = result.find('{')
            json_end = result.rfind('}')
            
            if json_start == -1 or json_end == -1:
                logger.warning("无法从AI响应中提取JSON")
                return None
            
            json_str = result[json_start:json_end+1]
            parsed = json.loads(json_str)
            
            if not isinstance(parsed, dict):
                logger.warning("AI返回的JSON格式不是字典")
                return None
            
            return parsed
            
        except json.JSONDecodeError as e:
            logger.error(f"解析AI响应失败: {e}")
            logger.error(f"AI响应内容: {result[:500] if len(result) > 500 else result}")
            return None
        except Exception as e:
            logger.error(f"处理AI响应时发生错误: {e}")
            return None
    
    def _merge_ai_result_with_stats(
        self,
        ai_result: Dict,
        candidate_topics: List[Dict]
    ) -> List[Dict]:
        """
        将AI分析结果与统计数据合并
        
        Args:
            ai_result: AI分析结果
            candidate_topics: 候选话题列表
            
        Returns:
            合并后的话题列表
        """
        ai_topics = ai_result.get('hot_topics', [])
        
        if not ai_topics:
            return []
        
        stats_dict = {t['title']: t for t in candidate_topics}
        
        merged_topics = []
        for ai_topic in ai_topics:
            title = ai_topic.get('title', '')
            
            if title in stats_dict:
                stats = stats_dict[title]
                merged_topic = {
                    'rank': ai_topic.get('rank', len(merged_topics) + 1),
                    'title': title,
                    'appear_count': stats.get('appear_count', 0),
                    'best_rank': stats.get('best_rank'),
                    'avg_hot_value': stats.get('avg_hot_value'),
                    'max_hot_value': stats.get('max_hot_value'),
                    'first_appear_time': stats.get('first_appear_time'),
                    'last_appear_time': stats.get('last_appear_time'),
                    'is_persistent': ai_topic.get('is_persistent', True),
                    'persistence_reason': ai_topic.get('persistence_reason', stats.get('persistence_reason', ''))
                }
            else:
                merged_topic = {
                    'rank': ai_topic.get('rank', len(merged_topics) + 1),
                    'title': title,
                    'appear_count': 0,
                    'best_rank': None,
                    'avg_hot_value': None,
                    'max_hot_value': None,
                    'first_appear_time': None,
                    'last_appear_time': None,
                    'is_persistent': ai_topic.get('is_persistent', True),
                    'persistence_reason': ai_topic.get('persistence_reason', '')
                }
            
            merged_topics.append(merged_topic)
        
        return merged_topics
    
    def save_daily_summary(self, summary_result: Dict[str, Any]) -> Optional[int]:
        """
        保存每日热门话题总结到数据库
        
        Args:
            summary_result: 总结结果
            
        Returns:
            总结记录ID
        """
        if not summary_result:
            return None
        
        summary_date = summary_result.get('summary_date')
        topics = summary_result.get('topics', [])
        summary_text = summary_result.get('summary_text', '')
        
        if not summary_date:
            logger.error("缺少总结日期，无法保存")
            return None
        
        log_step("保存", f"保存每日热门话题总结到数据库...")
        
        return self.storage.save_daily_hot_topic_summary(
            summary_date=summary_date,
            topics=topics,
            summary_text=summary_text
        )
    
    def analyze_weekly_hot_topics(
        self,
        week_start: datetime = None,
        week_end: datetime = None
    ) -> Optional[Dict[str, Any]]:
        """
        分析每周热门话题，进行跨天综合分析
        
        Args:
            week_start: 周开始日期，默认为上周日
            week_end: 周结束日期，默认为本周六
            
        Returns:
            分析结果字典
        """
        if week_start is None or week_end is None:
            today = datetime.now()
            weekday = today.weekday()
            
            week_end = today - timedelta(days=weekday + 1)
            week_start = week_end - timedelta(days=6)
        
        week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
        week_end = week_end.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        log_step("每周总结", f"开始分析 {week_start.strftime('%Y-%m-%d')} 至 {week_end.strftime('%Y-%m-%d')} 的热门话题...")
        
        daily_summaries = self.storage.get_daily_summaries_for_week(week_start, week_end)
        
        if not daily_summaries:
            logger.warning(f"没有找到 {week_start.strftime('%Y-%m-%d')} 至 {week_end.strftime('%Y-%m-%d')} 的每日总结数据")
            return None
        
        logger.info(f"获取到 {len(daily_summaries)} 天的每日总结数据")
        
        topic_day_stats = {}
        for daily_summary in daily_summaries:
            summary_date = daily_summary['summary_date']
            for topic in daily_summary['topics']:
                title = topic['title']
                if title not in topic_day_stats:
                    topic_day_stats[title] = {
                        'appear_days': 0,
                        'daily_detail': {},
                        'first_appear_date': None,
                        'last_appear_date': None,
                        'best_rank': None,
                        'avg_rank_sum': 0,
                        'rank_count': 0
                    }
                
                topic_day_stats[title]['appear_days'] += 1
                topic_day_stats[title]['daily_detail'][summary_date] = {
                    'rank': topic['rank'],
                    'appear_count': topic.get('appear_count', 0),
                    'best_rank': topic.get('best_rank'),
                    'is_persistent': topic.get('is_persistent', False)
                }
                
                if topic_day_stats[title]['first_appear_date'] is None or summary_date < topic_day_stats[title]['first_appear_date']:
                    topic_day_stats[title]['first_appear_date'] = summary_date
                
                if topic_day_stats[title]['last_appear_date'] is None or summary_date > topic_day_stats[title]['last_appear_date']:
                    topic_day_stats[title]['last_appear_date'] = summary_date
                
                current_rank = topic['rank']
                if topic_day_stats[title]['best_rank'] is None or current_rank < topic_day_stats[title]['best_rank']:
                    topic_day_stats[title]['best_rank'] = current_rank
                
                topic_day_stats[title]['avg_rank_sum'] += current_rank
                topic_day_stats[title]['rank_count'] += 1
        
        candidate_topics = []
        for title, stats in topic_day_stats.items():
            appear_days = stats['appear_days']
            
            if appear_days >= 2:
                avg_rank = stats['avg_rank_sum'] / stats['rank_count'] if stats['rank_count'] > 0 else 999
                
                candidate_topics.append({
                    'title': title,
                    'appear_days': appear_days,
                    'daily_appear_detail': stats['daily_detail'],
                    'first_appear_date': stats['first_appear_date'],
                    'last_appear_date': stats['last_appear_date'],
                    'best_rank': stats['best_rank'],
                    'avg_rank': avg_rank
                })
        
        candidate_topics.sort(key=lambda x: (-x['appear_days'], x['avg_rank']))
        
        if not candidate_topics:
            logger.warning("没有找到符合条件的持续性热门话题")
            return None
        
        logger.info(f"筛选出 {len(candidate_topics)} 个候选持续性话题")
        
        ai_result = self._analyze_with_ai(
            target_date=(week_start, week_end),
            snapshots_info=daily_summaries,
            candidate_topics=candidate_topics,
            analysis_type='weekly'
        )
        
        if ai_result:
            final_topics = self._merge_weekly_ai_result_with_stats(ai_result, candidate_topics)
            summary_text = ai_result.get('summary_text', '')
            
            return {
                'week_start': week_start,
                'week_end': week_end,
                'topics': final_topics,
                'summary_text': summary_text,
                'total_daily_summaries': len(daily_summaries),
                'total_topics': len(final_topics)
            }
        else:
            final_topics = []
            for idx, topic in enumerate(candidate_topics[:20]):
                final_topics.append({
                    'rank': idx + 1,
                    'title': topic['title'],
                    'appear_days': topic['appear_days'],
                    'daily_appear_detail': topic.get('daily_appear_detail', {}),
                    'heat_trend': '未知（AI分析失败）',
                    'heat_evolution': 'AI分析失败，使用默认规则筛选',
                    'first_appear_date': topic.get('first_appear_date'),
                    'last_appear_date': topic.get('last_appear_date'),
                    'is_sustained': True,
                    'sustained_reason': f"本周出现 {topic['appear_days']} 天"
                })
            
            return {
                'week_start': week_start,
                'week_end': week_end,
                'topics': final_topics,
                'summary_text': 'AI分析失败，使用默认规则筛选的持续性热门话题',
                'total_daily_summaries': len(daily_summaries),
                'total_topics': len(final_topics)
            }
    
    def _merge_weekly_ai_result_with_stats(
        self,
        ai_result: Dict,
        candidate_topics: List[Dict]
    ) -> List[Dict]:
        """
        将每周AI分析结果与统计数据合并
        
        Args:
            ai_result: AI分析结果
            candidate_topics: 候选话题列表
            
        Returns:
            合并后的话题列表
        """
        ai_topics = ai_result.get('hot_topics', [])
        
        if not ai_topics:
            return []
        
        stats_dict = {t['title']: t for t in candidate_topics}
        
        merged_topics = []
        for ai_topic in ai_topics:
            title = ai_topic.get('title', '')
            
            if title in stats_dict:
                stats = stats_dict[title]
                merged_topic = {
                    'rank': ai_topic.get('rank', len(merged_topics) + 1),
                    'title': title,
                    'appear_days': stats.get('appear_days', 0),
                    'daily_appear_detail': stats.get('daily_appear_detail', {}),
                    'heat_trend': ai_topic.get('heat_trend', '稳定'),
                    'heat_evolution': ai_topic.get('heat_evolution', ''),
                    'first_appear_date': stats.get('first_appear_date'),
                    'last_appear_date': stats.get('last_appear_date'),
                    'is_sustained': ai_topic.get('is_sustained', True),
                    'sustained_reason': ai_topic.get('sustained_reason', f"本周出现 {stats.get('appear_days', 0)} 天")
                }
            else:
                merged_topic = {
                    'rank': ai_topic.get('rank', len(merged_topics) + 1),
                    'title': title,
                    'appear_days': 0,
                    'daily_appear_detail': {},
                    'heat_trend': ai_topic.get('heat_trend', '稳定'),
                    'heat_evolution': ai_topic.get('heat_evolution', ''),
                    'first_appear_date': None,
                    'last_appear_date': None,
                    'is_sustained': ai_topic.get('is_sustained', True),
                    'sustained_reason': ai_topic.get('sustained_reason', '')
                }
            
            merged_topics.append(merged_topic)
        
        return merged_topics
    
    def save_weekly_summary(self, summary_result: Dict[str, Any]) -> Optional[int]:
        """
        保存每周热门话题总结到数据库
        
        Args:
            summary_result: 总结结果
            
        Returns:
            总结记录ID
        """
        if not summary_result:
            return None
        
        week_start = summary_result.get('week_start')
        week_end = summary_result.get('week_end')
        topics = summary_result.get('topics', [])
        summary_text = summary_result.get('summary_text', '')
        
        if week_start is None or week_end is None:
            logger.error("缺少周周期日期，无法保存")
            return None
        
        log_step("保存", f"保存每周热门话题总结到数据库...")
        
        return self.storage.save_weekly_hot_topic_summary(
            week_start=week_start,
            week_end=week_end,
            topics=topics,
            summary_text=summary_text
        )
    
    def run_daily_summary(self, target_date: datetime = None) -> Optional[Dict[str, Any]]:
        """
        执行完整的每日总结流程
        
        Args:
            target_date: 目标日期
            
        Returns:
            总结结果
        """
        logger.info("=" * 60)
        logger.info("执行每日热门话题总结")
        logger.info("=" * 60)
        
        result = self.analyze_daily_hot_topics(target_date)
        
        if result:
            summary_id = self.save_daily_summary(result)
            
            if summary_id:
                result['summary_id'] = summary_id
                logger.info(f"每日总结执行成功，总结ID: {summary_id}")
            else:
                logger.warning("每日总结分析完成，但保存失败")
        else:
            logger.warning("每日总结分析失败")
        
        return result
    
    def run_weekly_summary(
        self,
        week_start: datetime = None,
        week_end: datetime = None
    ) -> Optional[Dict[str, Any]]:
        """
        执行完整的每周总结流程
        
        Args:
            week_start: 周开始日期
            week_end: 周结束日期
            
        Returns:
            总结结果
        """
        logger.info("=" * 60)
        logger.info("执行每周热门话题总结")
        logger.info("=" * 60)
        
        result = self.analyze_weekly_hot_topics(week_start, week_end)
        
        if result:
            summary_id = self.save_weekly_summary(result)
            
            if summary_id:
                result['summary_id'] = summary_id
                logger.info(f"每周总结执行成功，总结ID: {summary_id}")
            else:
                logger.warning("每周总结分析完成，但保存失败")
        else:
            logger.warning("每周总结分析失败")
        
        return result
