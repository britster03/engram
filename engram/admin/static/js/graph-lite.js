(function (global) {
  function render(canvas, graph, options) {
    options = options || {};
    var ctx = canvas.getContext('2d');
    var dpr = window.devicePixelRatio || 1;
    var rect = canvas.getBoundingClientRect();
    canvas.width = Math.max(1, Math.floor(rect.width * dpr));
    canvas.height = Math.max(1, Math.floor(rect.height * dpr));
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    var width = rect.width;
    var height = rect.height;
    var nodes = (graph.nodes || []).map(function (n, i) {
      var angle = (i / Math.max(1, graph.nodes.length)) * Math.PI * 2;
      return Object.assign({}, n, {
        x: width / 2 + Math.cos(angle) * Math.min(width, height) * 0.28,
        y: height / 2 + Math.sin(angle) * Math.min(width, height) * 0.28,
        vx: 0,
        vy: 0
      });
    });
    var byId = new Map(nodes.map(function (n) { return [n.id, n]; }));
    var edges = (graph.edges || []).filter(function (e) {
      return byId.has(e.source) && byId.has(e.target);
    });
    var dragging = null;

    function colorFor(node) {
      if (node.status === 'LOW_CONFIDENCE') return '#b45309';
      if (node.status === 'HISTORICAL') return '#737373';
      if (node.node_type === 'ENTITY') return '#2563eb';
      if (node.node_type === 'FACT') return '#7c3aed';
      if (node.node_type === 'SESSION_SUMMARY') return '#059669';
      return '#111827';
    }

    function displayLabel(node) {
      var raw = node.label || node.source_uri || node.id || '';
      var source = node.source_uri || raw;
      if (node.node_type === 'ENTITY' && source.indexOf('/entities/') !== -1) {
        var parts = source.split('/').filter(Boolean);
        raw = parts.length >= 2 ? parts[parts.length - 2] : parts[parts.length - 1];
      } else if (source.indexOf('/episodes/') !== -1) {
        raw = source.split('/').pop() || source;
        raw = raw.replace(/^\d{4}-\d{2}-\d{2}_/, '');
      }
      raw = String(raw).replace('mem://', '').replace(/\.md$/, '');
      var max = nodes.length <= 3 ? 24 : 32;
      if (raw.length > max) raw = raw.slice(0, max - 3) + '...';
      return raw;
    }

    function tick() {
      var repulsion = 2800;
      var spring = 0.012;
      for (var i = 0; i < nodes.length; i++) {
        for (var j = i + 1; j < nodes.length; j++) {
          var a = nodes[i], b = nodes[j];
          var dx = a.x - b.x, dy = a.y - b.y;
          var dist2 = Math.max(64, dx * dx + dy * dy);
          var force = repulsion / dist2;
          var dist = Math.sqrt(dist2);
          var fx = (dx / dist) * force;
          var fy = (dy / dist) * force;
          a.vx += fx; a.vy += fy;
          b.vx -= fx; b.vy -= fy;
        }
      }
      edges.forEach(function (e) {
        var a = byId.get(e.source), b = byId.get(e.target);
        var dx = b.x - a.x, dy = b.y - a.y;
        var dist = Math.max(1, Math.sqrt(dx * dx + dy * dy));
        var target = 130;
        var f = (dist - target) * spring;
        var fx = (dx / dist) * f, fy = (dy / dist) * f;
        a.vx += fx; a.vy += fy;
        b.vx -= fx; b.vy -= fy;
      });
      nodes.forEach(function (n) {
        if (n === dragging) return;
        n.vx += (width / 2 - n.x) * 0.002;
        n.vy += (height / 2 - n.y) * 0.002;
        n.vx *= 0.82;
        n.vy *= 0.82;
        n.x = Math.max(24, Math.min(width - 24, n.x + n.vx));
        n.y = Math.max(24, Math.min(height - 24, n.y + n.vy));
      });
    }

    function draw() {
      ctx.clearRect(0, 0, width, height);
      ctx.strokeStyle = '#d4d4d4';
      ctx.fillStyle = '#737373';
      ctx.font = '11px Inter, system-ui, sans-serif';
      edges.forEach(function (e) {
        var a = byId.get(e.source), b = byId.get(e.target);
        ctx.beginPath();
        ctx.moveTo(a.x, a.y);
        ctx.lineTo(b.x, b.y);
        ctx.stroke();
        if (e.label) {
          ctx.fillText(e.label, (a.x + b.x) / 2 + 4, (a.y + b.y) / 2 - 4);
        }
      });
      nodes.forEach(function (n) {
        ctx.beginPath();
        ctx.fillStyle = colorFor(n);
        ctx.arc(n.x, n.y, 9, 0, Math.PI * 2);
        ctx.fill();
        ctx.fillStyle = '#111827';
        ctx.font = '12px Inter, system-ui, sans-serif';
        var label = displayLabel(n);
        var labelWidth = ctx.measureText(label).width;
        var drawLeft = n.x > width * 0.58;
        var textX = drawLeft ? n.x - 13 : n.x + 13;
        var textY = n.y + (n.y > height - 42 ? -14 : 4);
        var boxX = drawLeft ? textX - labelWidth - 4 : textX - 4;
        var boxY = textY - 13;
        ctx.fillStyle = 'rgba(255, 255, 255, 0.86)';
        ctx.fillRect(boxX, boxY, labelWidth + 8, 18);
        ctx.fillStyle = '#111827';
        ctx.textAlign = drawLeft ? 'right' : 'left';
        ctx.fillText(label, textX, textY);
        ctx.textAlign = 'left';
      });
    }

    function frame() {
      for (var i = 0; i < 2; i++) tick();
      draw();
      canvas._graphAnimation = requestAnimationFrame(frame);
    }
    if (canvas._graphAnimation) cancelAnimationFrame(canvas._graphAnimation);
    frame();

    canvas.onmousemove = function (event) {
      if (!dragging) return;
      var box = canvas.getBoundingClientRect();
      dragging.x = event.clientX - box.left;
      dragging.y = event.clientY - box.top;
      dragging.vx = 0;
      dragging.vy = 0;
    };
    canvas.onmousedown = function (event) {
      var box = canvas.getBoundingClientRect();
      var x = event.clientX - box.left;
      var y = event.clientY - box.top;
      dragging = nodes.find(function (n) {
        var dx = n.x - x, dy = n.y - y;
        return dx * dx + dy * dy < 180;
      }) || null;
      if (dragging && options.onSelect) options.onSelect(dragging);
    };
    canvas.onmouseup = function () { dragging = null; };
    canvas.onclick = function (event) {
      var box = canvas.getBoundingClientRect();
      var x = event.clientX - box.left;
      var y = event.clientY - box.top;
      var hit = nodes.find(function (n) {
        var dx = n.x - x, dy = n.y - y;
        return dx * dx + dy * dy < 220;
      });
      if (hit && options.onSelect) options.onSelect(hit);
    };
    return { nodes: nodes, edges: edges };
  }
  global.EngramGraph = { render: render };
})(window);
