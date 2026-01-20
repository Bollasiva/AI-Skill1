import React, { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import { getDashboardData } from "../api/apiService";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import { motion } from "framer-motion";

const CustomTooltip = ({ active, payload, label }) => {
  if (active && payload && payload.length) {
    return (
      <div className="custom-tooltip">
        <p className="label">{`Year: ${label}`}</p>
        <p className="intro">{`Demand Score: ${payload[0].value}`}</p>
      </div>
    );
  }
  return null;
};

const SkillEvolutionDashboard = () => {
  const [trendData, setTrendData] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [selectedSkill, setSelectedSkill] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const { data } = await getDashboardData();
        // sort by latest demand score
        data.sort((a, b) => {
          const lastDemandA = a.history.length
            ? a.history[a.history.length - 1].demand_score
            : 0;
          const lastDemandB = b.history.length
            ? b.history[b.history.length - 1].demand_score
            : 0;
          return lastDemandB - lastDemandA;
        });

        setTrendData(data);
        if (data.length > 0) setSelectedSkill(data[0]);
      } catch (error) {
        console.error("Failed to fetch dashboard data:", error);
      } finally {
        setIsLoading(false);
      }
    };
    fetchData();
  }, []);

  const chartData = selectedSkill
    ? [...selectedSkill.history, ...selectedSkill.forecast]
    : [];

  if (isLoading) {
    return (
      <div
        className="glass-card"
        style={{ padding: "40px", textAlign: "center" }}
      >
        Loading Dashboard Data...
      </div>
    );
  }

  return (
    <motion.div
      className="glass-card profile-card"
      style={{ maxWidth: "900px" }}
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
    >
      <div className="profile-header">
        <h2>Skill Evolution Dashboard</h2>
        <Link to="/profile" className="back-link">
          Back to Profile
        </Link>
      </div>

      <div className="dashboard-content">
        <div className="skill-selector">
          <label htmlFor="skill-select">Select a Skill to Analyze:</label>
          <select
            id="skill-select"
            className="career-interest-select"
            onChange={(e) =>
              setSelectedSkill(
                trendData.find((t) => t.skill === e.target.value)
              )
            }
            value={selectedSkill?.skill || ""}
          >
            {trendData.map((t) => (
              <option key={t.skill} value={t.skill}>
                {t.skill.charAt(0).toUpperCase() + t.skill.slice(1)}
              </option>
            ))}
          </select>
        </div>

        {selectedSkill && (
          <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }}>
            <h3 className="chart-title">
              Demand for "{selectedSkill.skill}" Over Time
            </h3>
            <ResponsiveContainer width="100%" height={400}>
              <LineChart
                data={chartData}
                margin={{ top: 5, right: 20, left: -10, bottom: 5 }}
              >
                <CartesianGrid
                  strokeDasharray="3 3"
                  stroke="rgba(255, 255, 255, 0.2)"
                />
                <XAxis dataKey="year" stroke="#94a3b8" />
                <YAxis stroke="#94a3b8" />
                <Tooltip
                  content={<CustomTooltip />}
                  wrapperStyle={{
                    backgroundColor: "#1e293b",
                    border: "1px solid #334155",
                    borderRadius: "10px",
                  }}
                />
                <Legend />
                <Line
                  type="monotone"
                  dataKey="demand_score"
                  name="Historical Demand"
                  stroke="#8884d8"
                  strokeWidth={2}
                  data={selectedSkill.history}
                />
                <Line
                  type="monotone"
                  dataKey="demand_score"
                  name="Forecasted Demand"
                  stroke="#82ca9d"
                  strokeWidth={2}
                  strokeDasharray="5 5"
                  data={selectedSkill.forecast}
                />
              </LineChart>
            </ResponsiveContainer>
          </motion.div>
        )}
      </div>
    </motion.div>
  );
};

export default SkillEvolutionDashboard;