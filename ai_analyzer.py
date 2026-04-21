#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AI分析模块 - 分析微博热搜趋势和股票交易机会
"""

import json
import os
from datetime import datetime
import requests


class AIAnalyzer:
    def __init__(self, config):
        self.config = config
        self.openrouter_config = config.get('openrouter', {})
        self.api_key = self.openrouter_config.get('api_key', '')
        self.api_url = self.openrouter_config.get('api_url', 'https://openrouter.ai/api/v1/chat/completions')
        self.model = self.openrouter_config.get('model', 'gpt-4o-mini')
    
    def _call_ai_api(self, prompt, system_prompt=None):
        """调用AI接口"""
        if not self.api_key:
            print("未配置OpenRouter API密钥")
            return None
        
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}',
            'HTTP-Referer': 'https://github.com/WeiboHotTradeSignal',
            'X-Title': '微博热搜交易信号分析器'
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
    
    def analyze_trend_changes(self, current_data, history_data):
        """分析热搜趋势变化"""
        print("开始分析热搜趋势变化...")
        
        # 准备当前数据
        current_hot_list = current_data.get('hot_list', []) if current_data else []
        current_time = current_data.get('timestamp', datetime.now().isoformat()) if current_data else datetime.now().isoformat()
        
        # 准备历史数据
        history_summary = []
        for idx, data in enumerate(history_data):
            hot_list = data.get('hot_list', [])
            timestamp = data.get('timestamp', f'历史数据{idx+1}')
            history_summary.append({
                'timestamp': timestamp,
                'top_10': [{'title': item['title'], 'rank': item['rank'], 'hot': item['hot']} 
                          for item in hot_list[:10]]
            })
        
        # 构建提示词
        system_prompt = """你是一位专业的数据分析专家，擅长分析社交媒体趋势和热点变化。
请根据提供的当前微博热搜数据和历史数据，分析热搜趋势的变化情况。
请以JSON格式返回分析结果，格式如下：
{
  "trend_analysis": {
    "new_hot_topics": [
      {"title": "热搜标题", "reason": "成为新热点的原因分析", "rank_change": "排名变化"}
    ],
    "rising_topics": [
      {"title": "热搜标题", "trend": "上升趋势描述", "potential_impact": "潜在影响"}
    ],
    "declining_topics": [
      {"title": "热搜标题", "trend": "下降趋势描述", "reason": "下降原因分析"}
    ],
    "keyword_evolution": {
      "emerging_keywords": ["新出现的关键词列表"],
      "fading_keywords": ["逐渐消失的关键词列表"],
      "stable_keywords": ["持续热门的关键词列表"]
    },
    "overall_trend_summary": "整体趋势变化的总结描述"
  }
}
"""
        
        prompt = f"""请分析以下微博热搜数据的趋势变化：

【当前时间】{current_time}

【当前热搜TOP 20】
{json.dumps([{'title': item['title'], 'rank': item['rank'], 'hot': item['hot']} 
            for item in current_hot_list[:20]], ensure_ascii=False, indent=2)}

【历史数据对比】
共 {len(history_summary)} 组历史数据：
{json.dumps(history_summary, ensure_ascii=False, indent=2)}

请分析：
1. 哪些是新出现的热门话题？
2. 哪些话题热度在快速上升？
3. 哪些话题热度在下降？
4. 关键词有什么演变趋势？
5. 整体趋势如何？

请以严格的JSON格式返回分析结果。"""
        
        result = self._call_ai_api(prompt, system_prompt)
        
        if result:
            try:
                # 尝试解析JSON
                # 有时候AI可能会在JSON前后添加其他文字，需要处理
                json_start = result.find('{')
                json_end = result.rfind('}')
                if json_start != -1 and json_end != -1:
                    json_str = result[json_start:json_end+1]
                    trend_analysis = json.loads(json_str)
                    return trend_analysis
                else:
                    print("无法从AI响应中提取JSON")
                    return None
            except json.JSONDecodeError as e:
                print(f"解析趋势分析结果失败: {e}")
                print(f"AI响应内容: {result[:500]}")
                return None
        else:
            return None
    
    def analyze_stock_opportunities(self, current_data, trend_analysis=None):
        """分析潜在股票交易机会"""
        print("开始分析潜在股票交易机会...")
        
        current_hot_list = current_data.get('hot_list', []) if current_data else []
        
        # 构建提示词
        system_prompt = """你是一位专业的股票分析师，擅长从社交媒体热点中发现潜在的股票交易机会。
请根据提供的微博热搜数据，分析可能影响股票市场的热点事件和潜在交易机会。
请以JSON格式返回分析结果，格式如下：
{
  "stock_opportunities": [
    {
      "event": "相关热点事件/话题",
      "impact_level": "影响程度（高/中/低）",
      "related_industries": ["关联行业1", "关联行业2"],
      "related_stocks": [
        {
          "stock_name": "股票名称",
          "stock_code": "股票代码",
          "industry": "所属行业",
          "reasoning": "关联原因分析",
          "signal_type": "买入信号/卖出信号/观察信号",
          "confidence": "信心程度（0-100）"
        }
      ],
      "analysis": "详细分析",
      "risk_warning": "风险提示"
    }
  ],
  "market_sentiment": {
    "overall_sentiment": "市场整体情绪（乐观/谨慎/悲观）",
    "sentiment_reason": "情绪判断理由",
    "hot_industries": ["当前热门行业列表"]
  },
  "summary": "整体分析总结"
}
"""
        
        prompt = f"""请分析以下微博热搜数据，找出可能影响股票市场的热点事件和潜在交易机会：

【当前热搜TOP 30】
{json.dumps([{'title': item['title'], 'rank': item['rank'], 'hot': item['hot']} 
            for item in current_hot_list[:30]], ensure_ascii=False, indent=2)}

【分析方向】
1. 识别可能影响股市的重大事件（政策发布、行业动态、公司新闻、突发事件等）
2. 分析事件可能影响的行业和具体股票
3. 判断交易信号类型（买入/卖出/观察）
4. 评估信心程度
5. 提供风险提示

【注意事项】
- 请基于客观事实分析，不要凭空猜测
- 关联行业和股票需要有合理的逻辑关联
- 信心程度应基于事件的影响力和确定性
- 请务必包含风险提示

请以严格的JSON格式返回分析结果。"""
        
        result = self._call_ai_api(prompt, system_prompt)
        
        if result:
            try:
                # 尝试解析JSON
                json_start = result.find('{')
                json_end = result.rfind('}')
                if json_start != -1 and json_end != -1:
                    json_str = result[json_start:json_end+1]
                    stock_analysis = json.loads(json_str)
                    return stock_analysis
                else:
                    print("无法从AI响应中提取JSON")
                    return None
            except json.JSONDecodeError as e:
                print(f"解析股票分析结果失败: {e}")
                print(f"AI响应内容: {result[:500]}")
                return None
        else:
            return None
    
    def run_analysis(self, current_data, history_data):
        """执行完整分析流程"""
        print("=" * 50)
        print("开始AI分析...")
        print("=" * 50)
        
        # 分析趋势变化
        trend_analysis = self.analyze_trend_changes(current_data, history_data)
        
        # 分析股票机会
        stock_analysis = self.analyze_stock_opportunities(current_data, trend_analysis)
        
        # 整合分析结果
        analysis_result = {
            'timestamp': datetime.now().isoformat(),
            'trend_analysis': trend_analysis,
            'stock_analysis': stock_analysis
        }
        
        print("=" * 50)
        print("AI分析完成！")
        print("=" * 50)
        
        return analysis_result


if __name__ == '__main__':
    # 测试代码
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    # 创建分析器
    analyzer = AIAnalyzer(config)
    
    # 准备测试数据
    test_current_data = {
        'timestamp': '2026-04-21T10:00:00',
        'hot_list': [
            {'rank': 1, 'title': '人工智能技术突破', 'hot': '500万', 'url': ''},
            {'rank': 2, 'title': '新能源汽车销量创新高', 'hot': '400万', 'url': ''},
            {'rank': 3, 'title': '央行降准', 'hot': '350万', 'url': ''},
            {'rank': 4, 'title': '芯片短缺问题缓解', 'hot': '300万', 'url': ''},
            {'rank': 5, 'title': '消费升级趋势明显', 'hot': '250万', 'url': ''}
        ]
    }
    
    test_history_data = [
        {
            'timestamp': '2026-04-20T10:00:00',
            'hot_list': [
                {'rank': 1, 'title': '新能源汽车销量创新高', 'hot': '380万', 'url': ''},
                {'rank': 2, 'title': '人工智能技术突破', 'hot': '350万', 'url': ''},
                {'rank': 3, 'title': '芯片短缺问题', 'hot': '280万', 'url': ''},
                {'rank': 4, 'title': '房地产政策', 'hot': '250万', 'url': ''},
                {'rank': 5, 'title': '医疗健康', 'hot': '200万', 'url': ''}
            ]
        }
    ]
    
    # 运行分析
    result = analyzer.run_analysis(test_current_data, test_history_data)
    
    if result:
        print(f"\n分析结果:")
        print(json.dumps(result, ensure_ascii=False, indent=2))
