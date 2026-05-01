#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HTML报告生成模块
"""

import os
import json
from datetime import datetime
from jinja2 import Environment, FileSystemLoader


class ReportGenerator:
    def __init__(self, config):
        self.config = config
        self.report_config = config.get('report', {})
        self.data_config = config.get('data', {})
        
        self.output_dir = self.report_config.get('output_dir', './reports')
        self.template_file = self.report_config.get('template_file', './templates/report_template.html')
        
        # 确保输出目录存在
        os.makedirs(self.output_dir, exist_ok=True)
    
    def _load_template(self):
        """加载模板"""
        template_dir = os.path.dirname(self.template_file)
        template_name = os.path.basename(self.template_file)
        
        env = Environment(loader=FileSystemLoader(template_dir))
        template = env.get_template(template_name)
        
        return template
    
    def _prepare_data(self, current_data, analysis_result):
        """准备模板数据"""
        report_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        current_hot_list = []
        if isinstance(current_data, dict):
            hot_list = current_data.get('hot_list', [])
            if isinstance(hot_list, list):
                current_hot_list = hot_list
        
        trend_analysis = None
        if isinstance(analysis_result, dict):
            ta = analysis_result.get('trend_analysis')
            if isinstance(ta, dict):
                trend_analysis = ta
        
        stock_opportunities = []
        market_sentiment = None
        report_summary = None
        
        if isinstance(analysis_result, dict):
            stock_analysis = analysis_result.get('stock_analysis')
            if isinstance(stock_analysis, dict):
                so = stock_analysis.get('stock_opportunities')
                if isinstance(so, list):
                    stock_opportunities = so
                
                ms = stock_analysis.get('market_sentiment')
                if isinstance(ms, dict):
                    market_sentiment = ms
                
                rs = stock_analysis.get('summary')
                if isinstance(rs, str):
                    report_summary = rs
        
        template_data = {
            'report_time': report_time,
            'current_hot_list': current_hot_list[:50] if isinstance(current_hot_list, list) else [],
            'trend_analysis': trend_analysis,
            'stock_opportunities': stock_opportunities if isinstance(stock_opportunities, list) else [],
            'market_sentiment': market_sentiment,
            'report_summary': report_summary
        }
        
        return template_data
    
    def generate_report(self, current_data, analysis_result, filename=None):
        """生成HTML报告"""
        print("开始生成HTML报告...")
        
        # 准备数据
        template_data = self._prepare_data(current_data, analysis_result)
        
        # 加载模板
        try:
            template = self._load_template()
        except Exception as e:
            print(f"加载模板失败: {e}")
            return None
        
        # 渲染模板
        try:
            html_content = template.render(**template_data)
        except Exception as e:
            print(f"渲染模板失败: {e}")
            return None
        
        # 生成文件名
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'report_{timestamp}.html'
        
        filepath = os.path.join(self.output_dir, filename)
        
        # 保存HTML文件
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(html_content)
            print(f"HTML报告已生成: {filepath}")
            return filepath
        except Exception as e:
            print(f"保存HTML报告失败: {e}")
            return None
    
    def save_analysis_result(self, analysis_result, filename=None):
        """保存分析结果到JSON文件"""
        if not analysis_result:
            print("没有分析结果可保存")
            return None
        
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'analysis_{timestamp}.json'
        
        filepath = os.path.join(self.output_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(analysis_result, f, ensure_ascii=False, indent=2)
            print(f"分析结果已保存: {filepath}")
            return filepath
        except Exception as e:
            print(f"保存分析结果失败: {e}")
            return None


if __name__ == '__main__':
    # 测试代码
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    # 创建报告生成器
    generator = ReportGenerator(config)
    
    # 准备测试数据
    test_current_data = {
        'timestamp': '2026-04-21T10:00:00',
        'hot_list': [
            {'rank': 1, 'title': '人工智能技术突破', 'hot': '500万', 'url': '', 'is_market': False},
            {'rank': 2, 'title': '新能源汽车销量创新高', 'hot': '400万', 'url': '', 'is_market': False},
            {'rank': 3, 'title': '央行降准', 'hot': '350万', 'url': '', 'is_market': False},
            {'rank': 4, 'title': '芯片短缺问题缓解', 'hot': '300万', 'url': '', 'is_market': False},
            {'rank': 5, 'title': '消费升级趋势明显', 'hot': '250万', 'url': '', 'is_market': False}
        ]
    }
    
    test_analysis_result = {
        'timestamp': '2026-04-21T10:05:00',
        'trend_analysis': {
            'new_hot_topics': [
                {'title': '人工智能技术突破', 'reason': 'AI技术取得重大突破，引发广泛关注', 'rank_change': '新上榜'}
            ],
            'rising_topics': [
                {'title': '新能源汽车销量创新高', 'trend': '热度持续上升，连续3天排名前5', 'potential_impact': '可能利好新能源汽车产业链'}
            ],
            'declining_topics': [
                {'title': '房地产政策', 'trend': '热度逐渐下降，已退出前10', 'reason': '政策影响逐渐消化'}
            ],
            'keyword_evolution': {
                'emerging_keywords': ['人工智能', 'AI技术', '技术突破'],
                'fading_keywords': ['房地产', '政策调控'],
                'stable_keywords': ['新能源汽车', '芯片', '消费升级']
            },
            'overall_trend_summary': '整体来看，科技类话题热度持续上升，人工智能和新能源汽车成为当前热点。传统行业话题热度有所下降。'
        },
        'stock_analysis': {
            'stock_opportunities': [
                {
                    'event': '人工智能技术突破',
                    'impact_level': '高',
                    'related_industries': ['人工智能', '计算机', '半导体'],
                    'related_stocks': [
                        {
                            'stock_name': '科大讯飞',
                            'stock_code': '002230',
                            'industry': '计算机应用',
                            'reasoning': '国内AI龙头企业，技术突破将直接利好公司发展',
                            'signal_type': '买入信号',
                            'confidence': 85
                        },
                        {
                            'stock_name': '寒武纪',
                            'stock_code': '688256',
                            'industry': '半导体',
                            'reasoning': 'AI芯片设计企业，技术突破将提升市场需求预期',
                            'signal_type': '买入信号',
                            'confidence': 80
                        }
                    ],
                    'analysis': '人工智能技术取得重大突破，将推动整个AI产业链的发展。从算法到算力，从应用到数据，各个环节都将受益。建议关注具有核心技术优势的龙头企业。',
                    'risk_warning': '技术突破的商业化落地仍需时间，短期市场反应可能过度。建议投资者保持理性，关注公司基本面和估值水平。',
                    'signal_type': '买入信号'
                },
                {
                    'event': '新能源汽车销量创新高',
                    'impact_level': '高',
                    'related_industries': ['新能源汽车', '锂电池', '汽车零部件'],
                    'related_stocks': [
                        {
                            'stock_name': '比亚迪',
                            'stock_code': '002594',
                            'industry': '汽车整车',
                            'reasoning': '国内新能源汽车龙头，销量数据亮眼',
                            'signal_type': '买入信号',
                            'confidence': 82
                        },
                        {
                            'stock_name': '宁德时代',
                            'stock_code': '300750',
                            'industry': '电力设备',
                            'reasoning': '全球动力电池龙头，将直接受益于新能源汽车销量增长',
                            'signal_type': '买入信号',
                            'confidence': 78
                        }
                    ],
                    'analysis': '新能源汽车销量持续创新高，表明行业景气度仍然较高。政策支持和技术进步推动行业快速发展，建议关注产业链优质企业。',
                    'risk_warning': '行业竞争激烈，价格战可能影响企业盈利。同时，原材料价格波动也会带来成本压力。',
                    'signal_type': '买入信号'
                }
            ],
            'market_sentiment': {
                'overall_sentiment': '乐观',
                'sentiment_reason': '科技类热点持续，市场情绪较为积极。但需警惕热点切换带来的波动风险。',
                'hot_industries': ['人工智能', '新能源汽车', '半导体', '消费电子']
            },
            'summary': '综合分析，当前市场热点集中在科技领域，人工智能和新能源汽车相关话题热度较高。建议投资者关注相关行业的优质标的，但需保持理性，做好风险控制。'
        }
    }
    
    # 生成报告
    html_path = generator.generate_report(test_current_data, test_analysis_result)
    
    if html_path:
        print(f"\n报告生成成功: {html_path}")
    
    # 保存分析结果
    json_path = generator.save_analysis_result(test_analysis_result)
    
    if json_path:
        print(f"分析结果保存成功: {json_path}")
