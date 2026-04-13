import { memo, useMemo } from 'react'
import ReactFlow, {
  Background,
  Controls,
  Handle,
  MarkerType,
  Position,
  type Edge,
  type Node,
  type NodeProps,
} from 'reactflow'

import 'reactflow/dist/style.css'

import type { PipelineStage, PipelineStageName } from '../../types/api'
import { Badge, Card, CardContent, CardDescription, CardHeader, CardTitle } from '../ui'
import { formatDuration, formatPercent, titleCase } from '../../lib/utils'

const ORDER: PipelineStageName[] = [
  'ingest',
  'reconstruction_schema_planner',
  'structure_reconstruction',
  'schema_analysis',
  'data_profiling',
  'anomaly_detection',
  'cleaning',
  'validation',
  'confidence_scoring',
]

function nodeColors(status: PipelineStage['status']) {
  switch (status) {
    case 'success':
      return 'border-emerald-500/40 bg-emerald-500/10'
    case 'failed':
      return 'border-rose-500/40 bg-rose-500/10'
    case 'partial':
      return 'border-amber-500/40 bg-amber-500/10'
    default:
      return 'border-slate-300 bg-white dark:border-slate-700 dark:bg-slate-950'
  }
}

type PipelineNodeData = {
  stage: PipelineStage
  selected: boolean
  onSelect: (stageName: string) => void
}

const PipelineNode = memo(({ data }: NodeProps<PipelineNodeData>) => (
  <button
    onClick={() => data.onSelect(data.stage.name)}
    className={`min-w-[220px] rounded-2xl border p-4 text-left shadow-lg shadow-slate-950/5 transition hover:-translate-y-0.5 ${nodeColors(data.stage.status)} ${
      data.selected ? 'ring-2 ring-sky-500' : ''
    }`}
  >
    <Handle type="target" position={Position.Left} className="!bg-sky-500" />
    <p className="text-sm font-semibold">{titleCase(data.stage.name)}</p>
    <div className="mt-3 flex flex-wrap gap-2 text-xs">
      <Badge variant={data.stage.status === 'success' ? 'success' : data.stage.status === 'partial' ? 'warning' : 'neutral'}>
        {data.stage.status}
      </Badge>
      <Badge variant="neutral">{formatDuration(data.stage.duration_ms)}</Badge>
      <Badge variant="info">{formatPercent(data.stage.confidence_score)}</Badge>
    </div>
    <Handle type="source" position={Position.Right} className="!bg-sky-500" />
  </button>
))
PipelineNode.displayName = 'PipelineNode'

const nodeTypes = { pipelineNode: PipelineNode }

export function PipelineGraph({
  stages,
  selectedStage,
  onSelectStage,
}: {
  stages: Record<string, PipelineStage>
  selectedStage: string | null
  onSelectStage: (stageName: string) => void
}) {
  const flowNodes = useMemo<Node<PipelineNodeData>[]>(
    () =>
      ORDER.map((stageName, index) => ({
        id: stageName,
        position: { x: index * 250, y: 40 },
        type: 'pipelineNode',
        data: {
          stage:
            stages[stageName] ?? {
              name: stageName,
              status: 'idle',
            },
          selected: selectedStage === stageName,
          onSelect: onSelectStage,
        },
      })),
    [onSelectStage, selectedStage, stages],
  )

  const flowEdges = useMemo<Edge[]>(
    () =>
      ORDER.slice(0, -1).map((stageName, index) => ({
        id: `${stageName}->${ORDER[index + 1]}`,
        source: stageName,
        target: ORDER[index + 1],
        markerEnd: { type: MarkerType.ArrowClosed },
        animated: true,
        style: { stroke: '#0ea5e9', strokeWidth: 1.5 },
      })),
    [],
  )

  const activeStage = selectedStage ? stages[selectedStage] : null

  return (
    <div className="grid gap-4 xl:grid-cols-[1fr_320px]">
      <Card>
        <CardHeader>
          <div>
            <CardTitle>Pipeline Visualization</CardTitle>
            <CardDescription>
              Inspect stage status, execution time, and confidence across the multi-agent flow.
            </CardDescription>
          </div>
        </CardHeader>
        <CardContent className="h-[520px]">
          <ReactFlow nodes={flowNodes} edges={flowEdges} nodeTypes={nodeTypes} fitView>
            <Controls />
            <Background />
          </ReactFlow>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Stage Details</CardTitle>
          <CardDescription>Click a node to inspect backend-generated stage summary data.</CardDescription>
        </CardHeader>
        <CardContent>
          {activeStage ? (
            <div className="space-y-4">
              <div className="space-y-1">
                <p className="text-lg font-semibold">{titleCase(activeStage.name)}</p>
                <div className="flex gap-2">
                  <Badge variant="neutral">{activeStage.status}</Badge>
                  <Badge variant="info">{formatDuration(activeStage.duration_ms)}</Badge>
                </div>
              </div>
              <div className="space-y-2">
                {Object.entries(activeStage.summary ?? {}).map(([key, value]) => (
                  <div key={key} className="rounded-xl bg-slate-100 px-3 py-2 text-sm dark:bg-slate-900">
                    <span className="font-medium">{titleCase(key)}:</span> {String(value)}
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <p className="text-sm text-slate-500 dark:text-slate-400">Select a stage to inspect details.</p>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
