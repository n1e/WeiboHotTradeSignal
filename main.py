#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
微博热搜交易信号分析器 - 主程序
"""

import json
import os
import sys
import argparse
from datetime import datetime

from weibo_scraper import WeiboScraper
from ai_analyzer import AIAnalyzer
from report_generator import ReportGenerator


def load_config(config_path='config.json'):
    """加载配置文件"""
    if not os.path.exists(config_path):
        print(f"配置文件不存在: {config_path}")
        print(f"请复制 config.example.json 为 config.json 并填写配置信息")
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


def check_config(config):
    """检查配置是否完整"""
    errors = []
    
    # 检查微博配置
    weibo_config = config.get('weibo', {})
    if not weibo_config.get('cookie_sub'):
        errors.append("缺少微博cookie_sub配置")
    
    # 检查OpenRouter配置
    openrouter_config = config.get('openrouter', {})
    if not openrouter_config.get('api_key'):
        errors.append("缺少OpenRouter API密钥配置")
    
    if errors:
        print("配置检查失败:")
        for error in errors:
            print(f"  - {error}")
        return False
    
    return True


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='微博热搜交易信号分析器')
    parser.add_argument('-c', '--config', default='config.json', help='配置文件路径')
    parser.add_argument('--skip-scrape', action='store_true', help='跳过数据采集，使用已有数据')
    parser.add_argument('--skip-analysis', action='store_true', help='跳过AI分析')
    parser.add_argument('--skip-report', action='store_true', help='跳过报告生成')
    parser.add_argument('--test', action='store_true', help='使用测试数据运行')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("微博热搜交易信号分析器")
    print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # 加载配置
    print("\n[1/4] 加载配置文件...")
    config = load_config(args.config)
    
    if not config:
        print("配置加载失败，程序退出")
        sys.exit(1)
    
    print(f"配置文件加载成功: {args.config}")
    
    # 测试模式使用模拟数据
    if args.test:
        print("\n⚠️  测试模式：将使用模拟数据运行")
        return run_test_mode(config)
    
    # 检查配置（非测试模式）
    if not check_config(config):
        print("配置不完整，程序退出")
        sys.exit(1)
    
    # 步骤1: 数据采集
    current_data = None
    history_data = []
    
    if not args.skip_scrape:
        print("\n[2/4] 采集微博热搜数据...")
        
        scraper = WeiboScraper(config)
        
        # 采集当前数据
        current_data = scraper.run()
        
        if not current_data:
            print("数据采集失败，程序退出")
            sys.exit(1)
        
        # 获取历史数据
        history_data = scraper.get_history_data()
        print(f"获取到 {len(history_data)} 条历史数据")
    else:
        print("\n[2/4] 跳过数据采集，尝试加载已有数据...")
        
        # 尝试加载最新的数据
        storage_dir = config.get('data', {}).get('storage_dir', './data')
        
        if os.path.exists(storage_dir):
            data_files = []
            for filename in os.listdir(storage_dir):
                if filename.startswith('weibo_hot_') and filename.endswith('.json'):
                    filepath = os.path.join(storage_dir, filename)
                    data_files.append(filepath)
            
            if data_files:
                # 按修改时间排序，取最新的
                data_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
                
                # 加载最新的作为当前数据
                try:
                    with open(data_files[0], 'r', encoding='utf-8') as f:
                        current_data = json.load(f)
                    print(f"成功加载最新数据: {data_files[0]}")
                except Exception as e:
                    print(f"加载数据失败: {e}")
                    current_data = None
                
                # 加载历史数据
                for filepath in data_files[1:config.get('data', {}).get('history_days', 7)]:
                    try:
                        with open(filepath, 'r', encoding='utf-8') as f:
                            history_data.append(json.load(f))
                    except Exception as e:
                        print(f"加载历史数据失败: {e}")
                
                print(f"获取到 {len(history_data)} 条历史数据")
            else:
                print("没有找到已有数据，请先运行数据采集")
                sys.exit(1)
        else:
            print("数据目录不存在，请先运行数据采集")
            sys.exit(1)
    
    # 步骤2: AI分析
    analysis_result = None
    
    if not args.skip_analysis:
        print("\n[3/4] AI分析数据...")
        
        analyzer = AIAnalyzer(config)
        analysis_result = analyzer.run_analysis(current_data, history_data)
        
        if not analysis_result:
            print("AI分析失败，但继续生成报告")
    else:
        print("\n[3/4] 跳过AI分析")
    
    # 步骤3: 生成报告
    if not args.skip_report:
        print("\n[4/4] 生成HTML报告...")
        
        generator = ReportGenerator(config)
        
        # 生成HTML报告
        html_path = generator.generate_report(current_data, analysis_result)
        
        # 保存分析结果
        if analysis_result:
            json_path = generator.save_analysis_result(analysis_result)
        
        print("\n" + "=" * 60)
        print("处理完成！")
        print("=" * 60)
        
        if html_path:
            print(f"\n📄 HTML报告: {html_path}")
        
        if analysis_result:
            print(f"📊 分析结果: {json_path}")
        
        print("\n⚠️  免责声明：本报告仅供参考，不构成任何投资建议。")
        print("      股市有风险，投资需谨慎。")
    else:
        print("\n[4/4] 跳过报告生成")
    
    print("\n" + "=" * 60)
    print("程序执行完成")
    print("=" * 60)


def run_test_mode(config):
    """测试模式 - 使用模拟数据"""
    print("\n[测试模式] 准备模拟数据...")
    
    # 模拟当前数据
    current_data = {
        'timestamp': datetime.now().isoformat(),
        'hot_list': [
            {'rank': 1, 'title': '人工智能技术突破', 'hot': '500万', 'url': '', 'is_market': False},
            {'rank': 2, 'title': '新能源汽车销量创新高', 'hot': '400万', 'url': '', 'is_market': False},
            {'rank': 3, 'title': '央行降准', 'hot': '350万', 'url': '', 'is_market': False},
            {'rank': 4, 'title': '芯片短缺问题缓解', 'hot': '300万', 'url': '', 'is_market': False},
            {'rank': 5, 'title': '消费升级趋势明显', 'hot': '250万', 'url': '', 'is_market': False},
            {'rank': 6, 'title': '5G应用场景拓展', 'hot': '200万', 'url': '', 'is_market': False},
            {'rank': 7, 'title': '医疗健康政策利好', 'hot': '180万', 'url': '', 'is_market': False},
            {'rank': 8, 'title': '教育改革推进', 'hot': '150万', 'url': '', 'is_market': False},
            {'rank': 9, 'title': '环保政策收紧', 'hot': '120万', 'url': '', 'is_market': False},
            {'rank': 10, 'title': '金融科技发展', 'hot': '100万', 'url': '', 'is_market': False}
        ]
    }
    
    # 模拟历史数据
    history_data = [
        {
            'timestamp': '2026-04-20T10:00:00',
            'hot_list': [
                {'rank': 1, 'title': '新能源汽车销量创新高', 'hot': '380万', 'url': '', 'is_market': False},
                {'rank': 2, 'title': '人工智能技术突破', 'hot': '350万', 'url': '', 'is_market': False},
                {'rank': 3, 'title': '芯片短缺问题', 'hot': '280万', 'url': '', 'is_market': False},
                {'rank': 4, 'title': '房地产政策', 'hot': '250万', 'url': '', 'is_market': False},
                {'rank': 5, 'title': '医疗健康', 'hot': '200万', 'url': '', 'is_market': False}
            ]
        }
    ]
    
    # 模拟分析结果
    analysis_result = {
        'timestamp': datetime.now().isoformat(),
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
                },
                {
                    'event': '央行降准',
                    'impact_level': '高',
                    'related_industries': ['银行', '房地产', '金融'],
                    'related_stocks': [
                        {
                            'stock_name': '招商银行',
                            'stock_code': '600036',
                            'industry': '银行',
                            'reasoning': '降准将释放流动性，利好银行板块',
                            'signal_type': '观察信号',
                            'confidence': 65
                        }
                    ],
                    'analysis': '央行降准将释放流动性，对市场整体形成利好。但需关注政策落地效果和市场反应。',
                    'risk_warning': '降准利好可能已被市场提前消化，需警惕利好出尽的风险。',
                    'signal_type': '观察信号'
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
    
    print("\n[测试模式] 生成HTML报告...")
    
    # 生成报告
    generator = ReportGenerator(config)
    
    html_path = generator.generate_report(current_data, analysis_result)
    json_path = generator.save_analysis_result(analysis_result)
    
    print("\n" + "=" * 60)
    print("测试模式执行完成！")
    print("=" * 60)
    
    if html_path:
        print(f"\n📄 HTML报告: {html_path}")
    
    if json_path:
        print(f"📊 分析结果: {json_path}")
    
    print("\n⚠️  免责声明：本报告仅供参考，不构成任何投资建议。")
    print("      股市有风险，投资需谨慎。")
    
    print("\n" + "=" * 60)
    print("程序执行完成")
    print("=" * 60)


if __name__ == '__main__':
    main()
