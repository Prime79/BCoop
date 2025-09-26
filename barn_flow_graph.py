#!/usr/bin/env python3
"""Render an SVG flow diagram for the batches that reached a given barn."""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from analysis.barn_flow import BarnFlow, BarnFlowBuilder, BarnStateChange, ShipmentFlow

SVG_WIDTH = 1600
SVG_HEIGHT = 1000
TOP_SETTERS = 8
TOP_HATCHERS = 8
TOP_TRUCKS = 6


@dataclass
class AggregatedNode:
    key: str
    label: str
    weight: float
    members: List[str]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("barn", help="Barn identifier, e.g. 'Kisvarsany-barn-01'")
    parser.add_argument("--flow-log", default="flow_log.parquet", help="Path to flow_log.parquet")
    parser.add_argument("--db-path", default="hatchery_events.sqlite", help="Path to hatchery events SQLite DB")
    parser.add_argument("--cutoff", help="Optional ISO timestamp cutoff")
    parser.add_argument("--output", default="notebooks/notebooks/outputs/barn_flow.svg", help="Where to write the SVG")
    parser.add_argument("--title", help="Optional chart title override")
    args = parser.parse_args()

    cutoff = datetime.fromisoformat(args.cutoff) if args.cutoff else None
    builder = BarnFlowBuilder(Path(args.flow_log), Path(args.db_path))
    barn_flow = builder.build(args.barn, cutoff)
    svg = render_svg(barn_flow, title=args.title)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(svg, encoding="utf-8")
    print(f"Wrote {output_path}")



def render_svg(barn_flow: BarnFlow, title: Optional[str] = None) -> str:
    context = build_context(barn_flow)
    svg_lines: List[str] = []
    add = svg_lines.append
    add('<?xml version="1.0" encoding="UTF-8" standalone="no"?>')
    add(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{SVG_WIDTH}" height="{SVG_HEIGHT}" '
        f'viewBox="0 0 {SVG_WIDTH} {SVG_HEIGHT}" style="background:#0b1220">'
    )

    add(_svg_defs())

    # Draw groups before edges to keep them underneath the arcs.
    for group in context['groups']:
        add(_render_group(group))

    max_weight = max(edge['weight'] for edge in context['edges']) if context['edges'] else 1.0
    for edge in context['edges']:
        path_d = _bezier(edge['a'], edge['b'])
        width = _scale_weight(edge['weight'], max_weight)
        opacity = 0.75 if edge['stage'] != 'truck' else 0.85
        add(
            f'<path d="{path_d}" fill="none" stroke="{edge["color"]}" '
            f'stroke-width="{width:.2f}" opacity="{opacity:.2f}" marker-end="url(#arrow)"/>'
        )

    if context['highlight']:
        highlight = context['highlight']
        for i in range(len(highlight) - 1):
            a = highlight[i]
            b = highlight[i + 1]
            path_d = _bezier(a, b)
            add(
                '<path d="{d}" fill="none" stroke="#00e0ff" stroke-width="5" opacity="0.95" '
                'filter="url(#softGlow)" marker-end="url(#arrowBright)"/>'.format(d=path_d)
            )
        hx, hy = highlight[-2]
        add(
            '<circle cx="{x:.1f}" cy="{y:.1f}" r="34" fill="none" stroke="#00e0ff" stroke-width="2" '
            'opacity="0.6" filter="url(#softGlow)"/>'.format(x=hx, y=hy)
        )

    for key in _ordered_node_keys(context['nodes']):
        add(_render_node(context['nodes'][key]))

    add(_render_title(title or context['title']))
    add(_render_legend())
    add(context['timeline'])
    add('</svg>')
    return ''.join(f"{line}\n" for line in svg_lines).rstrip('\n')


def build_context(barn_flow: BarnFlow) -> Dict[str, object]:
    cart_entries = _collect_cart_entries(barn_flow)
    setter_nodes, setter_map = _aggregate_stage(
        _sum_by(cart_entries, 'setter_machine', 'barn_eggs'),
        labeler=_format_setter,
        top_n=TOP_SETTERS,
        other_key='setter-other',
        other_label='Egyéb előkeltetők',
    )
    hatcher_nodes, hatcher_map = _aggregate_stage(
        _sum_by(cart_entries, 'hatcher_machine', 'barn_chicks'),
        labeler=_format_hatcher,
        top_n=TOP_HATCHERS,
        other_key='hatcher-other',
        other_label='Egyéb utókeltetők',
    )

    truck_totals, truck_shipment_totals = _collect_truck_deliveries(barn_flow.state_events)
    truck_nodes, truck_map = _aggregate_stage(
        truck_totals,
        labeler=_format_truck,
        top_n=TOP_TRUCKS,
        other_key='truck-other',
        other_label='Egyéb kamionok',
    )

    nodes = _layout_nodes(setter_nodes, hatcher_nodes, truck_nodes)

    total_eggs = sum(float(entry['barn_eggs']) for entry in cart_entries)
    nodes['eggs']['weight'] = total_eggs
    nodes['eggs']['label'] = 'Tojás'
    nodes['barn']['weight'] = barn_flow.current_occupancy
    nodes['barn']['label'] = barn_flow.barn_id

    edges = []
    edges.extend(_build_source_edges(cart_entries, setter_map))
    edges.extend(_build_stage_edges(cart_entries, setter_map, hatcher_map))
    edges.extend(
        _build_hatcher_truck_edges(
            barn_flow.shipments,
            cart_entries,
            hatcher_map,
            truck_map,
            truck_shipment_totals,
        )
    )
    edges.extend(_build_truck_barn_edges(truck_totals, truck_map))

    highlight = _compute_highlight_path(barn_flow, nodes, setter_map, hatcher_map, truck_map, truck_shipment_totals)
    groups = _build_groups(nodes)
    timeline = _render_timeline(barn_flow.state_events, nodes['barn'])

    return {
        'edges': edges,
        'nodes': nodes,
        'groups': groups,
        'highlight': highlight,
        'timeline': timeline,
        'title': f"{barn_flow.barn_id} · {barn_flow.cutoff.date().isoformat()}",
    }


def _collect_cart_entries(barn_flow: BarnFlow) -> List[Dict[str, object]]:
    entries: List[Dict[str, object]] = []
    for shipment in barn_flow.shipments:
        for cart in shipment.cart_contributions:
            setter = cart.setter_id.split('-cart')[0] if cart.setter_id else 'setter-unknown'
            hatcher = cart.hatcher_id.split('-cart')[0] if cart.hatcher_id else 'hatcher-unknown'
            entries.append(
                {
                    'shipment_id': shipment.shipment_id,
                    'setter_machine': setter,
                    'hatcher_machine': hatcher,
                    'barn_eggs': cart.barn_eggs,
                    'barn_chicks': cart.barn_chicks,
                }
            )
    return entries


def _collect_truck_deliveries(events: List[BarnStateChange]) -> Tuple[Dict[str, float], Dict[Tuple[str, str], float]]:
    totals: Dict[str, float] = defaultdict(float)
    by_shipment: Dict[Tuple[str, str], float] = defaultdict(float)
    for event in events:
        if not event.truck_id:
            continue
        for shipment_id, delta in event.shipment_deltas.items():
            if delta > 0:
                totals[event.truck_id] += delta
                by_shipment[(event.truck_id, shipment_id)] += delta
    return totals, by_shipment


def _build_source_edges(cart_entries: List[Dict[str, object]], setter_map: Dict[str, str]) -> List[Dict[str, object]]:
    accum: Dict[str, float] = defaultdict(float)
    for entry in cart_entries:
        setter_key = setter_map[entry['setter_machine']]
        weight = float(entry['barn_eggs']) or float(entry['barn_chicks'])
        accum[setter_key] += weight
    return [
        {
            'a': _node_anchor('eggs', 'right'),
            'b': _node_anchor(setter_key, 'left'),
            'weight': weight,
            'color': '#3b4556',
            'stage': 'setter',
        }
        for setter_key, weight in accum.items()
        if weight > 0
    ]


def _build_stage_edges(
    cart_entries: List[Dict[str, object]],
    setter_map: Dict[str, str],
    hatcher_map: Dict[str, str],
) -> List[Dict[str, object]]:
    accum: Dict[Tuple[str, str], float] = defaultdict(float)
    for entry in cart_entries:
        setter_key = setter_map[entry['setter_machine']]
        hatcher_key = hatcher_map[entry['hatcher_machine']]
        accum[(setter_key, hatcher_key)] += float(entry['barn_chicks'])
    return [
        {
            'a': _node_anchor(src, 'right'),
            'b': _node_anchor(dst, 'left'),
            'weight': weight,
            'color': '#4b566a',
            'stage': 'hatch',
        }
        for (src, dst), weight in accum.items()
        if weight > 0
    ]


def _build_hatcher_truck_edges(
    shipments: List[ShipmentFlow],
    cart_entries: List[Dict[str, object]],
    hatcher_map: Dict[str, str],
    truck_map: Dict[str, str],
    truck_shipment_totals: Dict[Tuple[str, str], float],
) -> List[Dict[str, object]]:
    cart_lookup: Dict[Tuple[str, str], float] = defaultdict(float)
    for entry in cart_entries:
        key = (entry['shipment_id'], entry['hatcher_machine'])
        cart_lookup[key] += float(entry['barn_chicks'])

    accum: Dict[Tuple[str, str], float] = defaultdict(float)
    for shipment in shipments:
        shipment_total = shipment.barn_quantity or 0.0
        if shipment_total <= 0:
            continue
        shipment_id = shipment.shipment_id
        for (truck_id, sid), qty in truck_shipment_totals.items():
            if sid != shipment_id or truck_id not in truck_map:
                continue
            share = qty / shipment_total if shipment_total else 0.0
            for cart in shipment.cart_contributions:
                hatcher_machine = cart.hatcher_id.split('-cart')[0] if cart.hatcher_id else 'hatcher-unknown'
                hatcher_key = hatcher_map[hatcher_machine]
                accum[(hatcher_key, truck_map[truck_id])] += cart.barn_chicks * share
    return [
        {
            'a': _node_anchor(src, 'right'),
            'b': _node_anchor(dst, 'left'),
            'weight': weight,
            'color': '#55617a',
            'stage': 'truck',
        }
        for (src, dst), weight in accum.items()
        if weight > 0
    ]


def _build_truck_barn_edges(truck_totals: Dict[str, float], truck_map: Dict[str, str]) -> List[Dict[str, object]]:
    return [
        {
            'a': _node_anchor(truck_map[truck_id], 'right'),
            'b': _node_anchor('barn', 'left'),
            'weight': weight,
            'color': '#5f6d85',
            'stage': 'truck',
        }
        for truck_id, weight in truck_totals.items()
        if truck_id in truck_map and weight > 0
    ]


def _node_anchor(node_key: str, side: str) -> Tuple[float, float]:
    node = NODE_REGISTRY[node_key]
    dx = 60.0 if side == 'right' else -60.0
    return node['x'] + dx, node['y']


NODE_REGISTRY: Dict[str, Dict[str, float]] = {}


def _layout_nodes(
    setter_nodes: List[AggregatedNode],
    hatcher_nodes: List[AggregatedNode],
    truck_nodes: List[AggregatedNode],
) -> Dict[str, Dict[str, float]]:
    global NODE_REGISTRY
    NODE_REGISTRY = {}
    columns = {
        'eggs': {'x': 120.0, 'nodes': [AggregatedNode('eggs', 'Tojás', 0.0, ['eggs'])]},
        'setter': {'x': 360.0, 'nodes': setter_nodes},
        'hatcher': {'x': 680.0, 'nodes': hatcher_nodes},
        'truck': {'x': 1000.0, 'nodes': truck_nodes},
        'barn': {'x': 1280.0, 'nodes': [AggregatedNode('barn', 'Istálló', 0.0, ['barn'])]},
    }
    layout: Dict[str, Dict[str, float]] = {}
    for column_id, spec in columns.items():
        x = spec['x']
        nodes = spec['nodes']
        y_positions = _column_positions(len(nodes))
        for idx, node in enumerate(nodes):
            y = y_positions[idx] if idx < len(y_positions) else SVG_HEIGHT / 2
            layout[node.key] = {
                'x': x,
                'y': y,
                'label': node.label,
                'column': column_id,
                'members': node.members,
                'weight': node.weight,
                'rank': idx,
            }
            NODE_REGISTRY[node.key] = layout[node.key]
    return layout


def _column_positions(count: int) -> List[float]:
    if count <= 0:
        return []
    if count == 1:
        return [SVG_HEIGHT / 2]
    spacing = min(140.0, 600.0 / (count - 1))
    start = (SVG_HEIGHT / 2) - spacing * (count - 1) / 2
    return [start + i * spacing for i in range(count)]



def _build_groups(nodes: Dict[str, Dict[str, float]]) -> List[str]:
    groups: List[str] = []
    group_specs = {
        'setter': {'title': 'Előkeltetők', 'keys': [k for k, v in nodes.items() if v['column'] == 'setter']},
        'hatcher': {'title': 'Utókeltetők', 'keys': [k for k, v in nodes.items() if v['column'] == 'hatcher']},
        'truck': {'title': 'Szállítás', 'keys': [k for k, v in nodes.items() if v['column'] == 'truck']},
        'barn': {'title': 'Cél istálló', 'keys': ['barn']},
    }
    for group_id, spec in group_specs.items():
        if not spec['keys']:
            continue
        margin_x = 110
        margin_y = 90
        xs = [nodes[key]['x'] for key in spec['keys']]
        ys = [nodes[key]['y'] for key in spec['keys']]
        x_min = min(xs) - margin_x
        x_max = max(xs) + margin_x
        y_min = min(ys) - margin_y
        y_max = max(ys) + margin_y
        width = x_max - x_min
        height = y_max - y_min
        title = spec['title']
        group_svg = (
            f'<g class="group" id="group-{group_id}">'
            f'<rect x="{x_min:.1f}" y="{y_min:.1f}" width="{width:.1f}" height="{height:.1f}" '
            f'rx="18" ry="18" fill="#0e172a" stroke="#2a3450" stroke-width="1.5" stroke-dasharray="6 6"/>'
            f'<text class="label" x="{(x_min + x_max) / 2:.1f}" y="{y_min - 16:.1f}" text-anchor="middle" font-size="15" opacity="0.9">{title}</text>'
            '</g>'
        )
        groups.append(group_svg)
    return groups






def _resolve_node_key(mapping: Dict[str, str], original: Optional[str], column: str) -> str:
    if original and original in mapping:
        candidate = mapping[original]
        if candidate in NODE_REGISTRY:
            return candidate
    for key, data in NODE_REGISTRY.items():
        if data['column'] == column:
            return key
    return next(iter(NODE_REGISTRY.keys()), column)

def _compute_highlight_path(
    barn_flow: BarnFlow,
    nodes: Dict[str, Dict[str, float]],
    setter_map: Dict[str, str],
    hatcher_map: Dict[str, str],
    truck_map: Dict[str, str],
    truck_shipment_totals: Dict[Tuple[str, str], float],
) -> List[Tuple[float, float]]:
    if not barn_flow.current_state:
        return []
    active_shipment = max(barn_flow.current_state.items(), key=lambda item: item[1])[0]
    shipment = next((s for s in barn_flow.shipments if s.shipment_id == active_shipment), None)
    if not shipment:
        return []
    top_cart = max(shipment.cart_contributions, key=lambda cart: cart.barn_chicks, default=None)
    if not top_cart or top_cart.barn_chicks <= 0:
        return []
    setter_machine = top_cart.setter_id.split('-cart')[0] if top_cart.setter_id else None
    hatcher_machine = top_cart.hatcher_id.split('-cart')[0] if top_cart.hatcher_id else None
    truck_id = None
    largest_qty = 0.0
    for (candidate, sid), qty in truck_shipment_totals.items():
        if sid == active_shipment and qty > largest_qty:
            largest_qty = qty
            truck_id = candidate
    setter_key = _resolve_node_key(setter_map, setter_machine, 'setter')
    hatcher_key = _resolve_node_key(hatcher_map, hatcher_machine, 'hatcher')
    path = [
        _node_anchor('eggs', 'right'),
        _node_anchor(setter_key, 'left'),
        _node_anchor(setter_key, 'right'),
        _node_anchor(hatcher_key, 'left'),
        _node_anchor(hatcher_key, 'right'),
    ]
    if truck_id:
        truck_key = _resolve_node_key(truck_map, truck_id, 'truck')
        path.append(_node_anchor(truck_key, 'left'))
        path.append(_node_anchor(truck_key, 'right'))
    path.append(_node_anchor('barn', 'left'))
    path.append((_node_anchor('barn', 'right')[0] + 40.0, _node_anchor('barn', 'right')[1]))
    return path


def _render_group(group_svg: str) -> str:
    return group_svg




def _ordered_node_keys(nodes: Dict[str, Dict[str, float]]) -> List[str]:
    order = ['eggs', 'setter', 'hatcher', 'truck', 'barn']
    result: List[str] = []
    for column in order:
        column_nodes = [(key, data) for key, data in nodes.items() if data['column'] == column]
        column_nodes.sort(key=lambda item: item[1].get('rank', 0))
        result.extend(key for key, _ in column_nodes)
    return result



def _render_node(node: Dict[str, float]) -> str:
    x = node['x']
    y = node['y']
    label = node['label']
    value = _format_quantity(node.get('weight', 0.0))
    lines = [
        f'<g class="node" transform="translate({x:.1f},{y:.1f})">',
        '<circle cx="0" cy="0" r="40" fill="url(#nodeGlow)" stroke="#8a93a6" stroke-width="1.2"/>',
        '<circle cx="0" cy="0" r="28" fill="#0f1a2e" stroke="#2c3650" stroke-width="2"/>',
        f'<text class="label" x="0" y="56" text-anchor="middle" font-size="13">{label}</text>',
    ]
    if value:
        lines.append(
            f'<text class="label" x="0" y="72" text-anchor="middle" font-size="12" opacity="0.75">{value}</text>'
        )
    lines.append('</g>')
    return ''.join(lines)



def _format_quantity(value: float) -> str:
    if value <= 0:
        return ''
    return f"{value:,.0f} db".replace(',', ' ')

def _render_title(title: str) -> str:
    return (
        f'<text class="label" x="80" y="72" font-size="24" fill="#f2f5ff" opacity="0.95">{title}</text>'
    )



def _render_legend() -> str:
    return """<g transform="translate(60,820)">
    <circle cx="0" cy="0" r="6" fill="#00e0ff"/>
    <text class="label" x="14" y="5" font-size="14">Kiemelt élő batch útvonala</text>
    <line x1="0" y1="30" x2="70" y2="30" stroke="#3b4556" stroke-width="3"/>
    <text class="label" x="82" y="35" font-size="14">Aggregált anyagáram</text>
    <rect x="-10" y="52" width="24" height="14" rx="3" ry="3" fill="#0e172a" stroke="#2a3450" stroke-width="1.5" stroke-dasharray="6 6"/>
    <text class="label" x="20" y="63" font-size="14">Csoport kerete</text>
</g>
"""

def _render_timeline(events: List[BarnStateChange], barn_node: Dict[str, float]) -> str:
    lines: List[str] = []
    for event in events[-8:]:
        for shipment_id, delta in sorted(event.shipment_deltas.items()):
            direction = '+' if delta > 0 else ''
            truck = f" · {event.truck_id}" if event.truck_id else ''
            lines.append(
                f"{event.timestamp.strftime('%Y-%m-%d %H:%M')} {direction}{int(delta)} {shipment_id}{truck}"
            )
    if not lines:
        lines.append('Nincs állapotváltozás a kiválasztott időszakban')
    x = barn_node['x'] + 120
    y = barn_node['y'] - 120
    svg_lines = [
        f'<g transform="translate({x:.1f},{y:.1f})">',
        '<text class="label" x="0" y="0" font-size="15" opacity="0.85">Istálló állapotváltozásai</text>',
    ]
    for idx, line in enumerate(lines):
        svg_lines.append(
            f'<text class="label" x="0" y="{24 + idx * 20}" font-size="13" opacity="0.75">{line}</text>'
        )
    svg_lines.append('</g>')
    return ''.join(svg_lines)



def _sum_by(entries: List[Dict[str, object]], key: str, value_key: str) -> Dict[str, float]:
    totals: Dict[str, float] = defaultdict(float)
    for entry in entries:
        totals[entry[key]] += float(entry[value_key])
    return totals


def _aggregate_stage(
    totals: Dict[str, float],
    *,
    labeler,
    top_n: int,
    other_key: str,
    other_label: str,
) -> Tuple[List[AggregatedNode], Dict[str, str]]:
    sorted_items = sorted(totals.items(), key=lambda item: item[1], reverse=True)
    nodes: List[AggregatedNode] = []
    mapping: Dict[str, str] = {}
    other_members: List[str] = []
    other_total = 0.0
    for idx, (key, weight) in enumerate(sorted_items):
        if idx < top_n or top_n <= 0:
            nodes.append(AggregatedNode(key=key, label=labeler(key), weight=weight, members=[key]))
            mapping[key] = key
        else:
            mapping[key] = other_key
            other_members.append(key)
            other_total += weight
    if other_members:
        label = f"{other_label} ({len(other_members)})"
        nodes.append(AggregatedNode(key=other_key, label=label, weight=other_total, members=other_members))
    if not nodes:
        nodes.append(AggregatedNode(key=other_key, label=other_label, weight=0.0, members=[]))
        mapping[other_key] = other_key
    return nodes, mapping


def _format_setter(key: str) -> str:
    return f"Előkeltető {key.split('-')[-1]}"


def _format_hatcher(key: str) -> str:
    return f"Utókeltető {key.split('-')[-1]}"


def _format_truck(key: str) -> str:
    return f"Kamion {key.split('-')[-1]}"


def _svg_defs() -> str:
    return """<defs>
            <radialGradient id="nodeGlow" cx="50%" cy="50%" r="60%">
                <stop offset="0%" stop-color="#ffffff" stop-opacity="0.15"/>
                <stop offset="100%" stop-color="#00ddeb" stop-opacity="0.08"/>
            </radialGradient>
            <filter id="softGlow" x="-50%" y="-50%" width="200%" height="200%">
                <feGaussianBlur stdDeviation="6" result="blur"/>
                <feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>
            </filter>
            <marker id="arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
                <path d="M0,0 L6,3 L0,6 Z" fill="#9aa4b2" />
            </marker>
            <marker id="arrowBright" markerWidth="10" markerHeight="10" refX="7" refY="5" orient="auto">
                <path d="M0,0 L9,5 L0,10 Z" fill="#00e0ff" />
            </marker>
            <style>
                .label { font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto; fill:#c8cfdb; }
            </style>
        </defs>"""

def _bezier(a: Tuple[float, float], b: Tuple[float, float], bend: float = 0.35) -> str:
    x1, y1 = a
    x2, y2 = b
    cx1 = x1 + (x2 - x1) * bend
    cy1 = y1
    cx2 = x2 - (x2 - x1) * bend
    cy2 = y2
    return f"M{x1:.1f},{y1:.1f} C{cx1:.1f},{cy1:.1f} {cx2:.1f},{cy2:.1f} {x2:.1f},{y2:.1f}"


def _scale_weight(weight: float, max_weight: float) -> float:
    if max_weight <= 0:
        return 2.0
    base = 1.6
    span = 6.0
    return base + (weight / max_weight) * span


if __name__ == "__main__":
    main()
