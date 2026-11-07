import os
from PIL import Image, ImageDraw, ImageFont

def generate_stream_lineage_image():
    repo_dir = r"C:\Users\jerem\.gemini\antigravity\scratch\realtime-payment-pipeline"
    images_dir = os.path.join(repo_dir, "docs", "images")
    os.makedirs(images_dir, exist_ok=True)
    out_path = os.path.join(images_dir, "stream-lineage.png")
    
    width = 1100
    height = 500
    bg_color = (15, 23, 42) # Slate 900
    img = Image.new("RGBA", (width, height), bg_color)
    draw = ImageDraw.Draw(img)
    
    # Try default font
    try:
        font_title = ImageFont.truetype("arial.ttf", 22)
        font_node = ImageFont.truetype("arial.ttf", 15)
        font_sub = ImageFont.truetype("arial.ttf", 11)
        font_badge = ImageFont.truetype("arial.ttf", 10)
    except:
        font_title = font_node = font_sub = font_badge = ImageFont.load_default()
        
    # Title & Header
    draw.text((40, 25), "Confluent Cloud Stream Lineage", fill=(248, 250, 252), font=font_title)
    draw.text((40, 55), "Environment: payments-pipeline (env-c941a8)  |  Cluster: payments-cluster (lkc-w28k91)", fill=(148, 163, 184), font=font_sub)
    
    # Define Nodes
    # x, y, w, h, title, subtitle, badge_text, badge_color, border_color
    nodes = [
        # Col 1: Postgres
        {"id": "pg", "rect": (50, 190, 160, 90), "title": "PostgreSQL", "sub": "payments db (WAL)", "badge": "RUNNING", "bc": (34, 197, 94), "fill": (30, 41, 59)},
        # Col 2: Connector
        {"id": "cdc", "rect": (250, 190, 170, 90), "title": "Debezium CDC", "sub": "payments-postgres-cdc", "badge": "RUNNING", "bc": (34, 197, 94), "fill": (30, 41, 59)},
        # Col 3: Raw Topics
        {"id": "t_tx", "rect": (460, 120, 180, 80), "title": "transactions", "sub": "payments.public.tx", "badge": "3 PARTITIONS", "bc": (59, 130, 246), "fill": (30, 41, 59)},
        {"id": "t_acc", "rect": (460, 260, 180, 80), "title": "accounts", "sub": "payments.public.acc", "badge": "3 PARTITIONS", "bc": (59, 130, 246), "fill": (30, 41, 59)},
        # Col 4: Flink Enrichment
        {"id": "flink_enr", "rect": (680, 190, 150, 90), "title": "Flink: enrichment", "sub": "Changelog JOIN", "badge": "ACTIVE", "bc": (168, 85, 247), "fill": (45, 30, 65)},
        # Col 5: Enriched Topic
        {"id": "t_enr", "rect": (870, 195, 170, 80), "title": "payments.enriched", "sub": "Avro Schema v1", "badge": "STREAMING", "bc": (59, 130, 246), "fill": (30, 41, 59)},
        # Col 6 (Lower flow): Flink Fraud -> fraud_alerts
        {"id": "flink_fraud", "rect": (680, 340, 150, 80), "title": "Flink: fraud-rule", "sub": "TUMBLE (60s) count>=3", "badge": "ACTIVE", "bc": (239, 68, 68), "fill": (65, 30, 40)},
        {"id": "t_fraud", "rect": (870, 340, 170, 80), "title": "payments.fraud_alerts", "sub": "Alert sink topic", "badge": "EMITTING", "bc": (239, 68, 68), "fill": (65, 30, 40)},
    ]
    
    # Helper to draw rounded rectangle
    for n in nodes:
        x, y, w, h = n["rect"]
        draw.rounded_rectangle([x, y, x + w, y + h], radius=8, fill=n["fill"], outline=(71, 85, 105), width=2)
        # Badge
        draw.rounded_rectangle([x + w - 75, y + 8, x + w - 8, y + 22], radius=4, fill=n["bc"])
        draw.text((x + w - 70, y + 9), n["badge"], fill=(255, 255, 255), font=font_badge)
        # Titles
        draw.text((x + 12, y + 25), n["title"], fill=(241, 245, 249), font=font_node)
        draw.text((x + 12, y + 50), n["sub"], fill=(148, 163, 184), font=font_sub)
        
    # Connections (Arrows)
    arrows = [
        # pg -> cdc
        ((210, 235), (250, 235)),
        # cdc -> t_tx & t_acc
        ((420, 220), (460, 160)),
        ((420, 250), (460, 300)),
        # t_tx & t_acc -> flink_enr
        ((640, 160), (680, 220)),
        ((640, 300), (680, 250)),
        # flink_enr -> t_enr
        ((830, 235), (870, 235)),
        # t_enr -> flink_fraud
        ((955, 275), (955, 310)),
        ((955, 310), (755, 310)),
        ((755, 310), (755, 340)),
        # flink_fraud -> t_fraud
        ((830, 380), (870, 380))
    ]
    
    for start, end in arrows:
        draw.line([start, end], fill=(56, 189, 248), width=2)
        # small arrowhead
        ex, ey = end
        draw.polygon([(ex, ey), (ex - 6, ey - 4), (ex - 6, ey + 4)], fill=(56, 189, 248))
        
    img.save(out_path, "PNG")
    print(f"Stream Lineage image saved to: {out_path}")

if __name__ == "__main__":
    generate_stream_lineage_image()
