#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
投资题材挖掘模块 - 从当日热搜资讯中深度挖掘潜在投资题材
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

from duckdb_storage import DuckDBStorage
from ai_analyzer import AIAnalyzer
from logger import get_logger, log_step, log_error
from pusher.manager import PushManager


logger = get_logger()


class InvestmentTopicMiner:
    """投资题材挖掘器"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化投资题材挖掘器
        
        Args:
            config: 配置字典
        """
        self.config = config
        self.storage = DuckDBStorage(config)
        self.ai_analyzer = AIAnalyzer(config)
        
        self.mining_config = config.get('investment_mining', {})
        self.enabled = self.mining_config.get('enabled', True)
        self.daily_time = self.mining_config.get('daily', {}).get('time', '21:30')
    
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
    
    def get_daily_hot_titles_for_mining(self, target_date: datetime = None) -> Optional[Dict[str, Any]]:
        """
        获取当日所有快照的热搜标题列表，用于投资题材挖掘
        
        Args:
            target_date: 目标日期，默认为今天
            
        Returns:
            包含标题列表的字典
        """
        if target_date is None:
            target_date = datetime.now()
        
        log_step("数据获取", f"获取 {target_date.strftime('%Y-%m-%d')} 的热搜标题列表...")
        
        titles_by_snapshot = self.storage.get_daily_titles_by_snapshot(target_date)
        
        if not titles_by_snapshot:
            logger.warning(f"没有找到 {target_date.strftime('%Y-%m-%d')} 的热搜数据")
            return None
        
        logger.info(f"共获取到 {len(titles_by_snapshot)} 条热搜记录（含所有快照）")
        
        unique_titles = self.storage.get_daily_unique_titles(target_date)
        logger.info(f"共统计到 {len(unique_titles)} 个不重复热搜话题")
        
        return {
            'target_date': target_date.strftime('%Y-%m-%d'),
            'titles_by_snapshot': titles_by_snapshot,
            'unique_titles': unique_titles,
            'total_snapshot_records': len(titles_by_snapshot),
            'total_unique_topics': len(unique_titles)
        }
    
    def _build_prompt_input(self, titles_data: Dict[str, Any]) -> str:
        """
        构建LLM输入：当日所有快照的完整热搜标题列表（只带时间点和标题，不带排名、链接等）
        
        Args:
            titles_data: 标题数据
            
        Returns:
            格式化的输入字符串
        """
        titles_by_snapshot = titles_data.get('titles_by_snapshot', [])
        unique_titles = titles_data.get('unique_titles', [])
        target_date = titles_data.get('target_date', '未知')
        
        input_str = f"【分析日期】{target_date}\n\n"
        
        input_str += "【当日所有热搜标题（按时间顺序，仅显示首次出现时间）】\n"
        input_str += "（注：以下为当日所有不重复的热搜话题，按首次出现时间排序）\n"
        input_str += "-" * 50 + "\n"
        
        for idx, topic in enumerate(unique_titles, 1):
            first_time = topic.get('first_appear_time', '')
            if first_time:
                try:
                    dt = datetime.fromisoformat(first_time)
                    time_str = dt.strftime('%H:%M')
                except:
                    time_str = first_time
            else:
                time_str = '未知时间'
            
            title = topic.get('title', '')
            appear_count = topic.get('appear_count', 1)
            
            input_str += f"{time_str} | {title}"
            if appear_count > 1:
                input_str += f"（出现{appear_count}次）"
            input_str += "\n"
        
        input_str += "\n" + "-" * 50 + "\n"
        input_str += f"\n【补充说明】\n"
        input_str += f"- 总快照记录数（含重复）: {titles_data.get('total_snapshot_records', 0)}\n"
        input_str += f"- 不重复话题数: {titles_data.get('total_unique_topics', 0)}\n"
        
        return input_str
    
    def analyze_investment_topics(self, titles_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        使用LLM深度挖掘潜在投资题材
        
        Args:
            titles_data: 标题数据
            
        Returns:
            分析结果字典
        """
        if not titles_data:
            logger.warning("没有标题数据，无法进行投资题材挖掘")
            return None
        
        target_date = titles_data.get('target_date', '未知')
        log_step("AI分析", f"开始挖掘 {target_date} 的投资题材...")
        
        system_prompt = """你是一位专业的资深股票分析师和投资研究员，擅长从社会热点、新闻资讯中深度挖掘潜在的投资机会和市场题材。

请基于提供的当日所有热搜标题列表，进行深度分析，挖掘潜在的投资题材。

【分析维度】
请从以下维度进行分析（但不限于）：
1. 政策导向：政府出台的新政策、法规、指导意见等
2. 行业动态：行业发展趋势、技术突破、产业链变化等
3. 公司事件：上市公司的重大公告、并购重组、业绩预告等
4. 突发事件：自然灾害、公共卫生事件、地缘政治冲突等
5. 消费爆款：新消费趋势、爆款产品、消费需求变化等
6. 自主可控：国产替代、卡脖子技术突破、安全可控相关等
7. 其他可能影响市场的重要事件

【输出格式】
请以严格的JSON格式返回分析结果，格式如下：
{
  "analysis_summary": "对当日整体投资环境的简要总结分析",
  "investment_topics": [
    {
      "topic_name": "题材名称（简洁明了，如"人工智能技术突破"）",
      "related_industries": ["相关行业列表，如"人工智能", "计算机", "半导体""],
      "core_logic": "核心投资逻辑分析，说明为什么这个题材值得关注",
      "potential_beneficiary_stocks": [
        {
          "stock_name": "股票名称",
          "stock_code": "股票代码（如适用）",
          "benefit_reason": "受益原因分析"
        }
      ],
      "market_expectation": "市场预期分析，包括短期影响、中长期趋势等",
      "analysis_dimension": "分析维度，如"政策导向"、"行业动态"、"公司事件"等",
      "related_hot_titles": ["相关的热搜标题列表，用于支撑分析"],
      "confidence_level": "信心程度：高/中/低"
    }
  ]
}

【重要要求】
1. 请基于提供的热搜标题进行客观分析，不要凭空猜测
2. LLM自主判断和分析，不做预处理筛选，所有题材都从原始数据中挖掘
3. 请尽量挖掘有价值的投资题材，而不是简单的热点罗列
4. 每个题材都需要有明确的逻辑支撑和相关的热搜标题
5. 请确保JSON格式严格正确，不要有语法错误
6. 请输出完整的JSON，包括所有字段
7. 如果当天没有明显的投资题材，请返回空的investment_topics数组，但analysis_summary需要说明原因"""
        
        prompt_input = self._build_prompt_input(titles_data)
        
        prompt = f"""请基于以下当日热搜标题列表，深度挖掘潜在的投资题材。

{prompt_input}

【分析要求】
1. 请仔细阅读所有热搜标题，从中挖掘可能影响股票市场的投资题材
2. 从政策导向、行业动态、公司事件、突发事件、消费爆款、自主可控等维度进行分析
3. 每个题材都需要有明确的核心逻辑、潜在受益标的、市场预期
4. 请客观分析，不要凭空猜测，所有分析都需要有相关热搜标题支撑
5. 请以严格的JSON格式返回结果

现在开始分析："""
        
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
            
            logger.info(f"AI分析完成，挖掘到 {len(parsed.get('investment_topics', []))} 个投资题材")
            
            return {
                'target_date': target_date,
                'analysis_time': datetime.now().isoformat(),
                'analysis_summary': parsed.get('analysis_summary', ''),
                'investment_topics': parsed.get('investment_topics', []),
                'raw_data_summary': {
                    'total_snapshot_records': titles_data.get('total_snapshot_records', 0),
                    'total_unique_topics': titles_data.get('total_unique_topics', 0)
                }
            }
            
        except json.JSONDecodeError as e:
            logger.error(f"解析AI响应失败: {e}")
            logger.error(f"AI响应内容: {result[:500] if len(result) > 500 else result}")
            return None
        except Exception as e:
            logger.error(f"处理AI响应时发生错误: {e}")
            return None
    
    def push_to_feishu(self, analysis_result: Dict[str, Any]) -> bool:
        """
        将分析结果推送到飞书
        
        Args:
            analysis_result: 分析结果
            
        Returns:
            是否推送成功
        """
        if not analysis_result:
            logger.warning("没有分析结果，无法推送")
            return False
        
        push_config = self.config.get('push', {})
        
        if not push_config.get('enabled', False):
            logger.info("推送功能已禁用，跳过")
            return True
        
        push_manager = PushManager(push_config)
        
        if not push_manager.is_available():
            logger.warning("没有可用的推送器")
            return False
        
        target_date = analysis_result.get('target_date', '未知')
        title = f"【投资题材挖掘】{target_date}"
        
        content = self._build_feishu_message_content(analysis_result)
        
        log_step("推送", "开始推送到飞书...")
        
        results = push_manager.push(title, content)
        
        if results:
            for pusher_name, success in results.items():
                if success:
                    logger.info(f"{pusher_name} 推送成功")
                else:
                    logger.warning(f"{pusher_name} 推送失败")
            return all(results.values())
        else:
            logger.warning("推送结果为空")
            return False
    
    def _build_feishu_message_content(self, analysis_result: Dict[str, Any]) -> str:
        """
        构建飞书消息内容
        
        Args:
            analysis_result: 分析结果
            
        Returns:
            消息内容字符串
        """
        lines = []
        
        target_date = analysis_result.get('target_date', '未知')
        analysis_time = analysis_result.get('analysis_time', '')
        
        lines.append(f"📊 **投资题材挖掘报告**")
        lines.append(f"📅 分析日期：{target_date}")
        if analysis_time:
            try:
                dt = datetime.fromisoformat(analysis_time)
                lines.append(f"⏰ 分析时间：{dt.strftime('%Y-%m-%d %H:%M:%S')}")
            except:
                pass
        lines.append("")
        
        analysis_summary = analysis_result.get('analysis_summary', '')
        if analysis_summary:
            lines.append("📝 **整体分析**")
            lines.append(analysis_summary)
            lines.append("")
        
        investment_topics = analysis_result.get('investment_topics', [])
        
        if not investment_topics:
            lines.append("⚠️ 当日未挖掘到明显的投资题材")
        else:
            lines.append(f"🎯 **挖掘到 {len(investment_topics)} 个投资题材**")
            lines.append("")
            
            for idx, topic in enumerate(investment_topics[:10], 1):
                topic_name = topic.get('topic_name', '未知题材')
                dimension = topic.get('analysis_dimension', '未分类')
                confidence = topic.get('confidence_level', '中')
                
                lines.append(f"{'='*40}")
                lines.append(f"**【{idx}】{topic_name}**")
                lines.append(f"   分类：{dimension} | 信心：{confidence}")
                lines.append("")
                
                related_industries = topic.get('related_industries', [])
                if related_industries:
                    lines.append(f"🏭 **相关行业**：{', '.join(related_industries)}")
                
                core_logic = topic.get('core_logic', '')
                if core_logic:
                    lines.append(f"💡 **核心逻辑**：")
                    lines.append(f"   {core_logic}")
                
                beneficiary_stocks = topic.get('potential_beneficiary_stocks', [])
                if beneficiary_stocks:
                    lines.append(f"📈 **潜在受益标的**：")
                    for stock in beneficiary_stocks[:3]:
                        stock_name = stock.get('stock_name', '')
                        stock_code = stock.get('stock_code', '')
                        benefit_reason = stock.get('benefit_reason', '')
                        
                        if stock_code:
                            lines.append(f"   - {stock_name}({stock_code})：{benefit_reason}")
                        else:
                            lines.append(f"   - {stock_name}：{benefit_reason}")
                
                market_expectation = topic.get('market_expectation', '')
                if market_expectation:
                    lines.append(f"🔮 **市场预期**：")
                    lines.append(f"   {market_expectation}")
                
                related_titles = topic.get('related_hot_titles', [])
                if related_titles:
                    lines.append(f"📰 **相关热搜**：")
                    for title in related_titles[:3]:
                        lines.append(f"   • {title}")
                
                lines.append("")
        
        lines.append("-" * 40)
        lines.append("⚠️ **免责声明**：")
        lines.append("本报告仅供参考，不构成任何投资建议。")
        lines.append("股市有风险，投资需谨慎。")
        
        return "\n".join(lines)
    
    def run_daily_mining(self, target_date: datetime = None) -> Optional[Dict[str, Any]]:
        """
        执行完整的每日投资题材挖掘流程
        
        Args:
            target_date: 目标日期，默认为今天
            
        Returns:
            挖掘结果
        """
        if target_date is None:
            target_date = datetime.now()
        
        logger.info("=" * 60)
        logger.info(f"执行每日投资题材挖掘 - {target_date.strftime('%Y-%m-%d')}")
        logger.info("=" * 60)
        
        titles_data = self.get_daily_hot_titles_for_mining(target_date)
        
        if not titles_data:
            logger.warning(f"没有找到 {target_date.strftime('%Y-%m-%d')} 的热搜数据，无法进行投资题材挖掘")
            return None
        
        analysis_result = self.analyze_investment_topics(titles_data)
        
        if not analysis_result:
            logger.warning("投资题材挖掘失败")
            return None
        
        push_success = self.push_to_feishu(analysis_result)
        
        if push_success:
            logger.info("投资题材挖掘结果已推送到飞书")
        else:
            logger.warning("投资题材挖掘结果推送失败")
        
        logger.info("=" * 60)
        logger.info("每日投资题材挖掘完成！")
        logger.info(f"挖掘到 {len(analysis_result.get('investment_topics', []))} 个投资题材")
        logger.info("=" * 60)
        
        return {
            'success': True,
            'target_date': target_date.strftime('%Y-%m-%d'),
            'analysis_result': analysis_result,
            'push_success': push_success
        }
