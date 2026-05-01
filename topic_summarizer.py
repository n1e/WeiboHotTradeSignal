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
        分析每日热门话题，让LLM从所有快照数据中选出最热的TOP 20
        
        Args:
            target_date: 目标日期，默认为昨天
            
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
            topic_stats = {}
        else:
            logger.info(f"共统计到 {len(topic_stats)} 个话题")
        
        ai_result = self._analyze_daily_with_ai(
            target_date=target_date,
            snapshots=snapshots,
            topic_stats=topic_stats
        )
        
        if ai_result:
            final_topics = self._merge_daily_ai_result_with_stats(ai_result, topic_stats)
            summary_text = ai_result.get('summary_text', '')
            
            return {
                'summary_date': target_date,
                'topics': final_topics,
                'summary_text': summary_text,
                'total_snapshots': len(snapshots),
                'total_topics': len(final_topics)
            }
        else:
            logger.warning("AI分析失败，无法生成每日总结")
            return None
    
    def _analyze_daily_with_ai(
        self,
        target_date: datetime,
        snapshots: List[Dict],
        topic_stats: Dict[str, Dict]
    ) -> Optional[Dict[str, Any]]:
        """
        使用AI分析每日热门话题，让LLM选出TOP 20
        
        Args:
            target_date: 目标日期
            snapshots: 快照列表
            topic_stats: 话题统计
            
        Returns:
            AI分析结果
        """
        log_step("AI分析", "开始调用AI分析每日热门话题...")
        
        system_prompt = """你是一位专业的社交媒体趋势分析专家。
请根据提供的微博热搜快照数据，分析当日最热的TOP 20热门话题。

分析标准：
1. 多次出现在热搜榜上的话题（出现次数多）
2. 排名靠前的话题（进入TOP 10，特别是TOP 5）
3. 热度值持续较高的话题

请以JSON格式返回分析结果，格式如下：
{
  "summary_text": "对当日热门话题的整体总结描述，包括主要热点类型、趋势特点等",
  "hot_topics": [
    {
      "rank": 1,
      "title": "话题标题",
      "heat_analysis": "对该话题热度的简要分析，说明为什么它是热门话题",
      "appear_analysis": "该话题在各快照中的出现情况分析"
    }
  ]
}

注意：
1. 请按热度和重要性从高到低排序，选出TOP 20
2. 确保JSON格式严格正确
3. 排名从1开始，连续编号"""
        
        snapshots_desc = ""
        for idx, snapshot in enumerate(snapshots):
            snapshot_time = snapshot['snapshot_time']
            hot_list = snapshot['hot_list']
            
            hot_items_str = ""
            for item in hot_list[:20]:
                hot_items_str += f"{item['rank']}.{item['title']};"
            
            snapshots_desc += f"\n【{snapshot_time}】{hot_items_str}\n"
        
        stats_desc = ""
        if topic_stats:
            stats_desc = "\n【话题统计参考】\n"
            sorted_topics = sorted(topic_stats.items(), key=lambda x: (-x[1]['appear_count'], x[1]['best_rank']))
            for idx, (title, stats) in enumerate(sorted_topics[:50]):
                stats_desc += f"{idx+1}. {title}\n"
                stats_desc += f"   出现次数: {stats['appear_count']}次, 最高排名: 第{stats['best_rank']}名\n"
        
        prompt = f"""请分析以下微博热搜数据，选出当日（{target_date.strftime('%Y-%m-%d')}）最热的TOP 20热门话题。

【快照数据】
（共{len(snapshots)}个代表性快照，每个快照包含时间和TOP热搜的排名+标题）
{snapshots_desc}
{stats_desc}
请完成以下任务：
1. 从所有快照数据中分析，选出当日最热的TOP 20热门话题
2. 按热度和重要性从高到低排序
3. 为每个话题提供热度分析和出现情况分析
4. 给出整体总结

请以严格的JSON格式返回结果。"""
        
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
    
    def _merge_daily_ai_result_with_stats(
        self,
        ai_result: Dict,
        topic_stats: Dict[str, Dict]
    ) -> List[Dict]:
        """
        将每日AI分析结果与统计数据合并
        
        Args:
            ai_result: AI分析结果
            topic_stats: 话题统计
            
        Returns:
            合并后的话题列表
        """
        ai_topics = ai_result.get('hot_topics', [])
        
        if not ai_topics:
            return []
        
        merged_topics = []
        for ai_topic in ai_topics[:20]:
            title = ai_topic.get('title', '')
            
            if title in topic_stats:
                stats = topic_stats[title]
                merged_topic = {
                    'rank': ai_topic.get('rank', len(merged_topics) + 1),
                    'title': title,
                    'appear_count': stats.get('appear_count', 0),
                    'best_rank': stats.get('best_rank'),
                    'avg_hot_value': stats.get('avg_hot_value'),
                    'max_hot_value': stats.get('max_hot_value'),
                    'first_appear_time': stats.get('first_appear_time'),
                    'last_appear_time': stats.get('last_appear_time'),
                    'is_persistent': True,
                    'persistence_reason': ai_topic.get('heat_analysis', ai_topic.get('appear_analysis', ''))
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
                    'is_persistent': True,
                    'persistence_reason': ai_topic.get('heat_analysis', ai_topic.get('appear_analysis', ''))
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
        分析每周热门话题，让LLM从每日总结中选出最热的TOP 20
        
        Args:
            week_start: 周开始日期
            week_end: 周结束日期
            
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
        
        ai_result = self._analyze_weekly_with_ai(
            week_start=week_start,
            week_end=week_end,
            daily_summaries=daily_summaries
        )
        
        if ai_result:
            final_topics = self._merge_weekly_ai_result_with_stats(ai_result, daily_summaries)
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
            logger.warning("AI分析失败，无法生成每周总结")
            return None
    
    def _analyze_weekly_with_ai(
        self,
        week_start: datetime,
        week_end: datetime,
        daily_summaries: List[Dict]
    ) -> Optional[Dict[str, Any]]:
        """
        使用AI分析每周热门话题，让LLM选出TOP 20
        
        Args:
            week_start: 周开始日期
            week_end: 周结束日期
            daily_summaries: 每日总结列表
            
        Returns:
            AI分析结果
        """
        log_step("AI分析", "开始调用AI分析每周热门话题...")
        
        system_prompt = """你是一位专业的社交媒体趋势分析专家。
请根据本周内的每日热门话题总结，进行跨天综合分析，识别一周内持续受到关注的话题，选出最热的TOP 20。

分析标准：
1. 连续多天出现在每日热门话题中的话题
2. 热度持续稳定或呈上升趋势的话题
3. 排名保持在较高位置的话题

请以JSON格式返回分析结果，格式如下：
{
  "summary_text": "对本周热门话题的整体总结描述，包括主要趋势、热点演变特点等",
  "hot_topics": [
    {
      "rank": 1,
      "title": "话题标题",
      "appear_days_analysis": "该话题在本周内的出现天数分析",
      "heat_trend": "热度趋势描述（上升/稳定/下降）",
      "heat_evolution": "详细的热度演变分析，包括每天的排名变化和热度变化"
    }
  ]
}

注意：
1. 请按持续性和重要性从高到低排序，选出TOP 20
2. 确保JSON格式严格正确
3. 排名从1开始，连续编号"""
        
        daily_summaries_desc = "\n【本周每日热门话题总结】\n"
        for idx, daily_summary in enumerate(daily_summaries):
            summary_date = daily_summary['summary_date']
            daily_summaries_desc += f"\n===== {summary_date} =====\n"
            
            for topic in daily_summary['topics'][:15]:
                daily_summaries_desc += f"{topic['rank']}.{topic['title']};"
            
            daily_summaries_desc += "\n"
        
        prompt = f"""请分析以下一周内的每日热门话题数据，进行跨天综合分析，选出本周最热的TOP 20持续性热门话题。

分析周期：{week_start.strftime('%Y-%m-%d')} 至 {week_end.strftime('%Y-%m-%d')}

{daily_summaries_desc}

请完成以下任务：
1. 从所有每日总结数据中分析，选出本周最热的TOP 20持续性热门话题
2. 分析每个持续性话题的热度演变趋势（上升/稳定/下降）
3. 按持续性和重要性从高到低排序
4. 为每个话题提供持续性分析和热度演变详情
5. 给出整体总结

请以严格的JSON格式返回结果。"""
        
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
    
    def _merge_weekly_ai_result_with_stats(
        self,
        ai_result: Dict,
        daily_summaries: List[Dict]
    ) -> List[Dict]:
        """
        将每周AI分析结果与统计数据合并
        
        Args:
            ai_result: AI分析结果
            daily_summaries: 每日总结列表
            
        Returns:
            合并后的话题列表
        """
        ai_topics = ai_result.get('hot_topics', [])
        
        if not ai_topics:
            return []
        
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
                        'best_rank': None
                    }
                
                topic_day_stats[title]['appear_days'] += 1
                topic_day_stats[title]['daily_detail'][summary_date] = {
                    'rank': topic['rank'],
                    'appear_count': topic.get('appear_count', 0),
                    'best_rank': topic.get('best_rank')
                }
                
                if topic_day_stats[title]['first_appear_date'] is None or summary_date < topic_day_stats[title]['first_appear_date']:
                    topic_day_stats[title]['first_appear_date'] = summary_date
                
                if topic_day_stats[title]['last_appear_date'] is None or summary_date > topic_day_stats[title]['last_appear_date']:
                    topic_day_stats[title]['last_appear_date'] = summary_date
                
                current_rank = topic['rank']
                if topic_day_stats[title]['best_rank'] is None or current_rank < topic_day_stats[title]['best_rank']:
                    topic_day_stats[title]['best_rank'] = current_rank
        
        merged_topics = []
        for ai_topic in ai_topics[:20]:
            title = ai_topic.get('title', '')
            
            if title in topic_day_stats:
                stats = topic_day_stats[title]
                merged_topic = {
                    'rank': ai_topic.get('rank', len(merged_topics) + 1),
                    'title': title,
                    'appear_days': stats.get('appear_days', 0),
                    'daily_appear_detail': stats.get('daily_detail', {}),
                    'heat_trend': ai_topic.get('heat_trend', '稳定'),
                    'heat_evolution': ai_topic.get('heat_evolution', ai_topic.get('appear_days_analysis', '')),
                    'first_appear_date': stats.get('first_appear_date'),
                    'last_appear_date': stats.get('last_appear_date'),
                    'is_sustained': True,
                    'sustained_reason': ai_topic.get('heat_evolution', ai_topic.get('appear_days_analysis', ''))
                }
            else:
                merged_topic = {
                    'rank': ai_topic.get('rank', len(merged_topics) + 1),
                    'title': title,
                    'appear_days': 0,
                    'daily_appear_detail': {},
                    'heat_trend': ai_topic.get('heat_trend', '稳定'),
                    'heat_evolution': ai_topic.get('heat_evolution', ai_topic.get('appear_days_analysis', '')),
                    'first_appear_date': None,
                    'last_appear_date': None,
                    'is_sustained': True,
                    'sustained_reason': ai_topic.get('heat_evolution', ai_topic.get('appear_days_analysis', ''))
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
